/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.delta.bigquery;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.Sequenced;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for BigQueryEventConsumer. In order to run these tests, service account credentials must be set in the system
 * properties. The service account must have permission to create and write to BigQuery datasets and tables,
 * as well as permission to write to GCS.
 *
 * The tests create real resources in GCP and will cost some small amount of money for each run.
 */
public class BigQueryEventConsumerTest {
  private static Storage storage;
  private static BigQuery bigQuery;
  private static String project;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    project = System.getProperty("project.id");
    Assume.assumeNotNull(project);

    String serviceAccountFilePath = System.getProperty("service.account.file");
    String serviceAccountContent = System.getProperty("service.account.content");
    Assume.assumeTrue(serviceAccountFilePath != null || serviceAccountContent != null);

    Credentials credentials;
    if (serviceAccountContent != null) {
      try (InputStream is = new ByteArrayInputStream(serviceAccountContent.getBytes(StandardCharsets.UTF_8))) {
        credentials = GoogleCredentials.fromStream(is)
          .createScoped("https://www.googleapis.com/auth/cloud-platform");
      }
    } else {
      File serviceAccountFile = new File(serviceAccountFilePath);
      try (InputStream is = new FileInputStream(serviceAccountFile)) {
        credentials = GoogleCredentials.fromStream(is)
          .createScoped("https://www.googleapis.com/auth/cloud-platform");
      }
    }

    bigQuery = BigQueryOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(project)
      .build()
      .getService();

    storage = StorageOptions.newBuilder()
      .setCredentials(credentials)
      .setProjectId(project)
      .build()
      .getService();
  }


  @Test
  public void testInsertUpdateDelete() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(NoOpContext.INSTANCE, storage, bigQuery,
                                                                    bucket, project, 100, 0, "_staging_", null);

    String dataset = "testInsertUpdateDelete";
    try {
      insertUpdateDelete(eventConsumer, dataset);
    } finally {
      for (Blob blob : bucket.list().iterateAll()) {
        storage.delete(blob.getBlobId());
      }
      bucket.delete();
      Dataset ds = bigQuery.getDataset(dataset);
      if (ds != null) {
        ds.delete(BigQuery.DatasetDeleteOption.deleteContents());
      }
    }
  }

  private void insertUpdateDelete(BigQueryEventConsumer eventConsumer, String dataset) throws Exception {
    // test creation of dataset
    DDLEvent createDatabase = DDLEvent.builder()
      .setOperation(DDLOperation.CREATE_DATABASE)
      .setDatabase(dataset)
      .build();

    eventConsumer.applyDDL(new Sequenced<>(createDatabase, 1L));
    Dataset ds = bigQuery.getDataset(dataset);
    Assert.assertNotNull(ds);

    // test creation of table
    String tableName = "users";
    Schema schema = Schema.recordOf("user",
                                    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("created", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("bday", Schema.of(Schema.LogicalType.DATE)),
                                    Schema.Field.of("score", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));
    DDLEvent createEvent = DDLEvent.builder()
      .setOperation(DDLOperation.CREATE_TABLE)
      .setDatabase(dataset)
      .setTable(tableName)
      .setSchema(schema)
      .setPrimaryKey(Collections.singletonList("id"))
      .build();
    eventConsumer.applyDDL(new Sequenced<>(createEvent, 2L));

    Table table = bigQuery.getTable(TableId.of(dataset, tableName));
    Assert.assertNotNull(table);
    TableDefinition tableDefinition = table.getDefinition();
    FieldList bqFields = tableDefinition.getSchema().getFields();
    Assert.assertEquals(LegacySQLTypeName.INTEGER, bqFields.get("id").getType());
    Assert.assertEquals(LegacySQLTypeName.STRING, bqFields.get("name").getType());
    Assert.assertEquals(LegacySQLTypeName.TIMESTAMP, bqFields.get("created").getType());
    Assert.assertEquals(LegacySQLTypeName.DATE, bqFields.get("bday").getType());
    Assert.assertEquals(LegacySQLTypeName.FLOAT, bqFields.get("score").getType());

    /*
        send events:
          insert <0, 'alice', 0L, 1970-01-01, 0.0>
          insert <1, 'bob', 86400L, 1970-01-02, 1.0>
          update id=0 to id=2
          delete id=1 (bob)

        should result in:
          2, 'alice', 1000L, 1970-01-01, 0.0
     */
    StructuredRecord insert1 = StructuredRecord.builder(schema)
      .set("id", 0)
      .set("name", "alice")
      .setTimestamp("created", ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")))
      .setDate("bday", LocalDate.ofEpochDay(0))
      .set("score", 0.0d)
      .build();
    DMLEvent insert1Event = DMLEvent.builder()
      .setOperation(DMLOperation.INSERT)
      .setIngestTimestamp(0L)
      .setSnapshot(false)
      .setDatabase(dataset)
      .setTable(tableName)
      .setRow(insert1)
      .setOffset(new Offset())
      .build();
    eventConsumer.applyDML(new Sequenced<>(insert1Event, 3L));

    StructuredRecord insert2 = StructuredRecord.builder(schema)
      .set("id", 1)
      .set("name", "bob")
      .setTimestamp("created", ZonedDateTime.ofInstant(Instant.ofEpochSecond(86400), ZoneId.of("UTC")))
      .setDate("bday", LocalDate.ofEpochDay(1))
      .set("score", 1.0d)
      .build();
    DMLEvent insert2Event = DMLEvent.builder()
      .setOperation(DMLOperation.INSERT)
      .setIngestTimestamp(1L)
      .setSnapshot(false)
      .setDatabase(dataset)
      .setTable(tableName)
      .setRow(insert2)
      .setOffset(new Offset())
      .build();
    eventConsumer.applyDML(new Sequenced<>(insert2Event, 4L));
    eventConsumer.flush();

    // should have 2 rows:
    // <0, 'alice', 0L, 1970-01-01, 0.0>
    // <1, 'bob', 86400L, 1970-01-02, 1.0>
    TableResult result = executeQuery(String.format("SELECT * from %s.%s ORDER BY id", dataset, tableName));
    Assert.assertEquals(2, result.getTotalRows());
    Iterator<FieldValueList> iter = result.iterateAll().iterator();
    FieldValueList row = iter.next();
    Assert.assertEquals(0L, row.get("id").getLongValue());
    Assert.assertEquals("alice", row.get("name").getStringValue());
    Assert.assertEquals(0L, row.get("created").getTimestampValue());
    Assert.assertEquals("1970-01-01", row.get("bday").getStringValue());
    Assert.assertEquals(0.0d, row.get("score").getDoubleValue(), 0.000001d);
    row = iter.next();
    Assert.assertEquals(1L, row.get("id").getLongValue());
    Assert.assertEquals("bob", row.get("name").getStringValue());
    Assert.assertEquals(TimeUnit.SECONDS.toMicros(86400), row.get("created").getTimestampValue());
    Assert.assertEquals("1970-01-02", row.get("bday").getStringValue());
    Assert.assertEquals(1.0d, row.get("score").getDoubleValue(), 0.000001d);

    StructuredRecord update = StructuredRecord.builder(schema)
      .set("id", 2)
      .set("name", insert1.get("name"))
      .setTimestamp("created", insert1.getTimestamp("created"))
      .setDate("bday", insert1.getDate("bday"))
      .set("score", insert1.get("score"))
      .build();
    DMLEvent updateEvent = DMLEvent.builder()
      .setOperation(DMLOperation.UPDATE)
      .setIngestTimestamp(2L)
      .setSnapshot(false)
      .setDatabase(dataset)
      .setTable(tableName)
      .setPreviousRow(insert1)
      .setRow(update)
      .setOffset(new Offset())
      .build();
    eventConsumer.applyDML(new Sequenced<>(updateEvent, 5L));

    DMLEvent deleteEvent = DMLEvent.builder()
      .setOperation(DMLOperation.DELETE)
      .setIngestTimestamp(3L)
      .setSnapshot(false)
      .setDatabase(dataset)
      .setTable(tableName)
      .setRow(insert2)
      .setOffset(new Offset())
      .build();
    eventConsumer.applyDML(new Sequenced<>(deleteEvent, 6L));
    eventConsumer.flush();

    // should have just one row: 0, 'Alice', 1000L, 1970-01-01, 0.0
    result = executeQuery(String.format("SELECT * from %s.%s", dataset, tableName));
    Assert.assertEquals(1L, result.getTotalRows());
    row = result.iterateAll().iterator().next();
    Assert.assertEquals(2L, row.get("id").getLongValue());
    Assert.assertEquals("alice", row.get("name").getStringValue());
    Assert.assertEquals(0L, row.get("created").getTimestampValue());
    Assert.assertEquals("1970-01-01", row.get("bday").getStringValue());
    Assert.assertEquals(0.0d, row.get("score").getDoubleValue(), 0.000001d);
  }

  private TableResult executeQuery(String query) throws InterruptedException {
    QueryJobConfiguration jobConfig = QueryJobConfiguration.of(query);
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigQuery.create(JobInfo.newBuilder(jobConfig).setJobId(jobId).build());
    queryJob.waitFor();
    return queryJob.getQueryResults();
  }
}
