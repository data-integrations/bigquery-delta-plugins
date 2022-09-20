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
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableSet;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests for BigQueryEventConsumer. In order to run these tests, service account credentials must be set in the system
 * properties. The service account must have permission to create and write to BigQuery datasets and tables,
 * as well as permission to write to GCS.
 *
 * The tests create real resources in GCP and will cost some small amount of money for each run.
 */
public class BigQueryEventConsumerTest {
  private static final String STAGING_TABLE_PREFIX = "_staging_";
  private static final Schema USER_SCHEMA = Schema.recordOf("user",
    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("created", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
    Schema.Field.of("bday", Schema.of(Schema.LogicalType.DATE)),
    Schema.Field.of("score", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("partition", Schema.nullableOf(Schema.of(Schema.Type.INT))));

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
  public void testCreateTableWithClustering() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));
    Map<String, String> runtimeArguments = new HashMap<>();
    runtimeArguments.put("gcp.bigquery.max.clustering.columns", "4");
    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(new MockContext(300, runtimeArguments), storage,
                                                                    bigQuery, bucket, project, 0, STAGING_TABLE_PREFIX,
                                                                    true, null, 1L, null, false);
    String dataset = "testTableCreationWithClustering";
    String tableName = "users";
    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add("id1");
    primaryKeys.add("id2");
    primaryKeys.add("id3");
    primaryKeys.add("id4");
    primaryKeys.add("id5");
    Schema schema = Schema.recordOf(tableName,
                                    Schema.Field.of("id1", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("id2", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("id3", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("id4", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("id5", Schema.of(Schema.Type.INT)));
    TableId tableId = TableId.of(dataset, tableName);

    bigQuery.create(DatasetInfo.newBuilder(dataset).build());
    DDLEvent createTable = DDLEvent.builder()
      .setOperation(DDLOperation.Type.CREATE_TABLE)
      .setDatabaseName(dataset)
      .setTableName(tableName)
      .setSchema(schema)
      .setPrimaryKey(primaryKeys)
      .setOffset(new Offset())
      .build();
    eventConsumer.applyDDL(new Sequenced<>(createTable, 0));

    Table table = bigQuery.getTable(tableId);
    StandardTableDefinition tableDefinition = table.getDefinition();
    Clustering clustering = tableDefinition.getClustering();
    Assert.assertNotNull(clustering);
    Assert.assertEquals(primaryKeys.subList(0, 4), clustering.getFields());
    bigQuery.delete(tableId);
    cleanupTest(bucket, dataset, eventConsumer);
  }

  @Test
  public void testHandleCreateTableAlreadyExists() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));
    Map<String, String> runtimeArguments = new HashMap<>();
    runtimeArguments.put("gcp.bigquery.max.clustering.columns", "4");
    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(new MockContext(300, runtimeArguments), storage,
            bigQuery, bucket, project, 0, STAGING_TABLE_PREFIX,
            true, null, 1L, null, false);
    String dataset = "testTableCreationWithClustering_" + UUID.randomUUID().toString().replaceAll("-", "_");
    String tableName = "users";
    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add("id1");
    primaryKeys.add("id2");
    primaryKeys.add("id3");
    primaryKeys.add("id4");
    primaryKeys.add("id5");
    Schema schema = Schema.recordOf(tableName,
            Schema.Field.of("id1", Schema.of(Schema.Type.INT)),
            Schema.Field.of("id2", Schema.of(Schema.Type.INT)),
            Schema.Field.of("id3", Schema.of(Schema.Type.INT)),
            Schema.Field.of("id4", Schema.of(Schema.Type.INT)),
            Schema.Field.of("id5", Schema.of(Schema.Type.INT)));
    TableId tableId = TableId.of(dataset, tableName);

    StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(Schemas.convert(schema))
            .build();
    TableInfo.Builder builder = TableInfo.newBuilder(tableId, tableDefinition);
    TableInfo tableInfo = builder.build();

    bigQuery.create(DatasetInfo.newBuilder(dataset).build());
    bigQuery.create(tableInfo);

    DDLEvent createTable = DDLEvent.builder()
            .setOperation(DDLOperation.Type.CREATE_TABLE)
            .setDatabaseName(dataset)
            .setTableName(tableName)
            .setSchema(schema)
            .setPrimaryKey(primaryKeys)
            .setOffset(new Offset())
            .build();

    eventConsumer.applyDDL(new Sequenced<>(createTable, 0));

    Table table = bigQuery.getTable(tableId);
    Assert.assertNotNull(table);

    bigQuery.delete(tableId);
    cleanupTest(bucket, dataset, eventConsumer);
  }

  @Test
  public void testCreateTableWithInvalidTypesForClustering() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));
    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(new MockContext(300, Collections.emptyMap()),
                                                                    storage, bigQuery, bucket, project, 0,
                                                                    STAGING_TABLE_PREFIX, true, null, 1L, null, false);

    String dataset = "testInvalidTypesForClustering";
    String allinvalidsTableName = "allinvalids";
    TableId allInvalidsTable = TableId.of(dataset, allinvalidsTableName);
    String someInvalidsTableName = "someinvalids";
    TableId someInvalidsTable = TableId.of(dataset, someInvalidsTableName);

    try {
      bigQuery.create(DatasetInfo.newBuilder(dataset).build());

      // Primary keys with all un-supported types for clustering
      List<String> primaryKeys = new ArrayList<>();
      primaryKeys.add("id1");
      Schema schema = Schema.recordOf(allinvalidsTableName,
                                      Schema.Field.of("id1", Schema.of(Schema.Type.BYTES)));

      DDLEvent allInvalidsCreateTable = DDLEvent.builder()
        .setOperation(DDLOperation.Type.CREATE_TABLE)
        .setDatabaseName(dataset)
        .setTableName(allinvalidsTableName)
        .setSchema(schema)
        .setPrimaryKey(primaryKeys)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDDL(new Sequenced<>(allInvalidsCreateTable, 0));

      Table table = bigQuery.getTable(allInvalidsTable);
      StandardTableDefinition tableDefinition = table.getDefinition();
      Clustering clustering = tableDefinition.getClustering();
      // No clustering should be added
      Assert.assertNull(clustering);
      bigQuery.delete(allInvalidsTable);

      // Primary keys with some un-supported types for clustering
      primaryKeys = new ArrayList<>();
      primaryKeys.add("id1");
      primaryKeys.add("id2");
      primaryKeys.add("id3");
      primaryKeys.add("id4");
      primaryKeys.add("id5");
      schema = Schema.recordOf(allinvalidsTableName,
                               Schema.Field.of("id1", Schema.of(Schema.Type.BYTES)),
                               Schema.Field.of("id2", Schema.of(Schema.Type.BYTES)),
                               Schema.Field.of("id3", Schema.of(Schema.Type.BYTES)),
                               Schema.Field.of("id4", Schema.of(Schema.Type.BYTES)),
                               // add one valid clustering key
                               Schema.Field.of("id5", Schema.of(Schema.Type.INT)));

      DDLEvent someInvalidsTableCreate = DDLEvent.builder()
        .setOperation(DDLOperation.Type.CREATE_TABLE)
        .setDatabaseName(dataset)
        .setTableName(someInvalidsTableName)
        .setSchema(schema)
        .setPrimaryKey(primaryKeys)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDDL(new Sequenced<>(someInvalidsTableCreate, 0));

      table = bigQuery.getTable(someInvalidsTable);
      tableDefinition = table.getDefinition();
      clustering = tableDefinition.getClustering();
      Assert.assertNotNull(clustering);
      Assert.assertEquals(primaryKeys.subList(4, 5), clustering.getFields());
      bigQuery.delete(someInvalidsTable);
    } finally {
      cleanupTest(bucket, dataset, eventConsumer);
    }
  }

  @Test
  public void testManualDropRetries() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(new MockContext(300, new HashMap()), storage,
                                                                    bigQuery, bucket, project, 0, STAGING_TABLE_PREFIX,
                                                                    true, null, 1L, null, false);

    String dataset = "testManualDropRetries";
    String tableName = "users";
    TableId tableId = TableId.of(dataset, tableName);

    try {
      bigQuery.create(DatasetInfo.newBuilder(dataset).build());
      bigQuery.create(TableInfo.newBuilder(tableId, StandardTableDefinition.newBuilder().build()).build());

      DDLEvent dropTable = DDLEvent.builder()
        .setOperation(DDLOperation.Type.DROP_TABLE)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setOffset(new Offset())
        .build();

      ExecutorService executorService = Executors.newSingleThreadExecutor();
      Future<Void> future = executorService.submit(() -> {
        eventConsumer.applyDDL(new Sequenced<>(dropTable, 0));
        return null;
      });

      try {
        future.get(10, TimeUnit.SECONDS);
        Assert.fail("Should not have dropped the table automatically.");
      } catch (TimeoutException e) {
        // expected
      }

      bigQuery.delete(tableId);
      future.get(1, TimeUnit.MINUTES);
    } finally {
      cleanupTest(bucket, dataset, eventConsumer);
    }
  }

  @Test
  public void testManualDrops() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(MockContext.INSTANCE, storage, bigQuery, bucket,
                                                                    project, 0, STAGING_TABLE_PREFIX, true, null, null,
                                                                    null, false);

    String dataset = "testManualDrops";
    String tableName = "users";
    TableId tableId = TableId.of(dataset, tableName);

    try {
      bigQuery.create(DatasetInfo.newBuilder(dataset).build());
      bigQuery.create(TableInfo.newBuilder(tableId, StandardTableDefinition.newBuilder().build()).build());

      // dropping a table that doesn't exist should be fine
      DDLEvent dropTable = DDLEvent.builder()
        .setOperation(DDLOperation.Type.DROP_TABLE)
        .setDatabaseName(dataset)
        .setTableName(UUID.randomUUID().toString())
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDDL(new Sequenced<>(dropTable, 0));

      dropTable = DDLEvent.builder()
        .setOperation(DDLOperation.Type.DROP_TABLE)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setOffset(new Offset())
        .build();
      try {
        eventConsumer.applyDDL(new Sequenced<>(dropTable, 1));
        Assert.fail("Expected exception when dropping table that already exists");
      } catch (Exception e) {
        // expected
      }

      // manually drop the table and replay the event, which should now succeed
      bigQuery.delete(tableId);
      eventConsumer.applyDDL(new Sequenced<>(dropTable, 1));

      DDLEvent dropDatabase = DDLEvent.builder()
        .setOperation(DDLOperation.Type.DROP_DATABASE)
        .setDatabaseName(UUID.randomUUID().toString())
        .setOffset(new Offset())
        .build();
      // should be fine if the dataset doesn't already exist
      eventConsumer.applyDDL(new Sequenced<>(dropDatabase, 2));

      dropDatabase = DDLEvent.builder()
        .setOperation(DDLOperation.Type.DROP_DATABASE)
        .setDatabaseName(dataset)
        .setOffset(new Offset())
        .build();
      try {
        eventConsumer.applyDDL(new Sequenced<>(dropDatabase, 2));
        Assert.fail("Expected exception when dropping dataset that already exists");
      } catch (Exception e) {
        // expected
      }

      // manually drop the dataset and replay the event, which should now succeed
      bigQuery.delete(dataset);
      eventConsumer.applyDDL(new Sequenced<>(dropDatabase, 2));
    } finally {
      cleanupTest(bucket, dataset, eventConsumer);
    }
  }

  @Test
  public void testAlter() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(MockContext.INSTANCE, storage, bigQuery, bucket,
                                                                    project, 0, STAGING_TABLE_PREFIX, false, null, null,
                                                                    null, false);

    String dataset = "testAlter";
    String tableName = "users";

    try {
      long sequenceNum = createDatasetAndTable(eventConsumer, dataset, Collections.singletonList(tableName));

      // alter schema to add a nullable field and make a non-nullable field into a nullable field
      Schema updatedSchema =
        Schema.recordOf("user-new",
                        Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                        Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                        Schema.Field.of("created", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                        // alter bday to be nullable
                        Schema.Field.of("bday", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                        // add a new nullable age field
                        Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                        Schema.Field.of("score", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                        Schema.Field.of("partition", Schema.nullableOf(Schema.of(Schema.Type.INT))));
      DDLEvent alterEvent = DDLEvent.builder()
        .setOperation(DDLOperation.Type.ALTER_TABLE)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setSchema(updatedSchema)
        .setPrimaryKey(Collections.singletonList("id"))
        .setOffset(new Offset())
        .setSnapshot(false)
        .build();
      eventConsumer.applyDDL(new Sequenced<>(alterEvent, sequenceNum + 1));

      Table table = bigQuery.getTable(TableId.of(dataset, tableName));
      Assert.assertNotNull(table);
      TableDefinition tableDefinition = table.getDefinition();
      FieldList bqFields = tableDefinition.getSchema().getFields();
      Assert.assertEquals(LegacySQLTypeName.INTEGER, bqFields.get("id").getType());
      Assert.assertEquals(LegacySQLTypeName.STRING, bqFields.get("name").getType());
      Assert.assertEquals(LegacySQLTypeName.TIMESTAMP, bqFields.get("created").getType());
      Assert.assertEquals(LegacySQLTypeName.DATE, bqFields.get("bday").getType());
      Assert.assertEquals(Field.Mode.NULLABLE, bqFields.get("bday").getMode());
      Assert.assertEquals(LegacySQLTypeName.INTEGER, bqFields.get("age").getType());
      Assert.assertEquals(Field.Mode.NULLABLE, bqFields.get("age").getMode());
      Assert.assertEquals(LegacySQLTypeName.FLOAT, bqFields.get("score").getType());
      Assert.assertEquals(Field.Mode.NULLABLE, bqFields.get("score").getMode());
      Assert.assertEquals(LegacySQLTypeName.INTEGER, bqFields.get("partition").getType());
      Assert.assertEquals(Field.Mode.NULLABLE, bqFields.get("partition").getMode());
    } finally {
      cleanupTest(bucket, dataset, eventConsumer);
    }
  }

  @Test
  public void testInsertUpdateDelete() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(MockContext.INSTANCE, storage, bigQuery, bucket,
                                                                    project, 0, STAGING_TABLE_PREFIX, false, null, null,
                                                                    null, false);

    String dataset = "testInsertUpdateDelete";
    try {
      insertUpdateDelete(eventConsumer, dataset, false);
    } finally {
      cleanupTest(bucket, dataset, eventConsumer);
    }
  }

  @Test
  public void testInsertTruncate() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(MockContext.INSTANCE, storage, bigQuery, bucket,
                                                                    project, 0, STAGING_TABLE_PREFIX, false, null, null,
                                                                    null, false);

    String dataset = "testInsertTruncate";
    try {
      insertTruncate(eventConsumer, dataset);
    } finally {
      cleanupTest(bucket, dataset, eventConsumer);
    }
  }

  @Test
  public void testSoftDeletes() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(MockContext.INSTANCE, storage, bigQuery, bucket,
                                                                    project, 0, STAGING_TABLE_PREFIX, false, null, null,
                                                                    null, true);

    String dataset = "testInsertUpdateSoftDelete";
    try {
      insertUpdateDelete(eventConsumer, dataset, true);
    } finally {
      cleanupTest(bucket, dataset, eventConsumer);
    }
  }

  @Test
  public void testBigQuerySchemaNormalization() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));
    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(MockContext.INSTANCE, storage, bigQuery, bucket,
                                                                    project, 0, STAGING_TABLE_PREFIX, false, null, null,
                                                                    null, false);

    String dataset = "testSchemaNormalization";
    String tableName = "test_table";
    String fieldName = "field-with-dash";
    String normalizedFieldName = "field_with_dash";
    String fieldWithNumber = "1970-01-01";
    String normalizedFieldWithNumber = "_1970_01_01";

    try {
      // test CREATE_TABLE operation
      Schema schema = Schema.recordOf(tableName,
                                      Schema.Field.of(fieldName, Schema.of(Schema.Type.INT)));
      bigQuery.create(DatasetInfo.newBuilder(dataset).build());
      DDLEvent createTable = DDLEvent.builder()
        .setOperation(DDLOperation.Type.CREATE_TABLE)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setSchema(schema)
        .setPrimaryKey(Collections.singletonList(fieldName))
        .build();
      eventConsumer.applyDDL(new Sequenced<>(createTable, 0));

      Table table = bigQuery.getTable(TableId.of(dataset, tableName));
      Assert.assertNotNull(table);
      FieldList bqFields = table.getDefinition().getSchema().getFields();
      Field normalizedField = bqFields.get(normalizedFieldName);
      Assert.assertNotNull(normalizedField);
      Assert.assertEquals(LegacySQLTypeName.INTEGER, normalizedField.getType());

      // test ALTER_TABLE operation
      schema = Schema.recordOf(tableName,
                               Schema.Field.of(fieldName, Schema.of(Schema.Type.INT)),
                               Schema.Field.of(fieldWithNumber, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
      DDLEvent alterTable = DDLEvent.builder()
        .setOperation(DDLOperation.Type.ALTER_TABLE)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setSchema(schema)
        .setPrimaryKey(Collections.singletonList(fieldName))
        .build();
      eventConsumer.applyDDL(new Sequenced<>(alterTable, 0));
      table = bigQuery.getTable(TableId.of(dataset, tableName));
      Assert.assertNotNull(table);
      bqFields = table.getDefinition().getSchema().getFields();
      normalizedField = bqFields.get(normalizedFieldWithNumber);
      Assert.assertNotNull(normalizedField);
      Assert.assertEquals(LegacySQLTypeName.STRING, normalizedField.getType());
      eventConsumer.flush();
      
      // test INSERT Operation
      StructuredRecord record = StructuredRecord.builder(schema)
        .set(fieldName, 1)
        .set(fieldWithNumber, "test varchar")
        .build();
      DMLEvent insertEvent = DMLEvent.builder()
        .setOperationType(DMLOperation.Type.INSERT)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setRow(record)
        .build();
      eventConsumer.applyDML(new Sequenced<>(insertEvent, 1L));
      eventConsumer.flush();
      TableResult result = executeQuery(String.format("SELECT * from %s.%s", dataset, tableName));
      Assert.assertEquals(1, result.getTotalRows());

      // test TRUNCATE_TABLE Operation
      DDLEvent truncateTable = DDLEvent.builder()
        .setOperation(DDLOperation.Type.TRUNCATE_TABLE)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .build();
      eventConsumer.applyDDL(new Sequenced<>(truncateTable, 0));
      table = bigQuery.getTable(TableId.of(dataset, tableName));
      Assert.assertNotNull(table);
      result = executeQuery(String.format("SELECT * from %s.%s", dataset, tableName));
      Assert.assertEquals(0, result.getTotalRows());
    } finally {
      cleanupTest(bucket, dataset, eventConsumer);
    }
  }

  private void insertUpdateDelete(BigQueryEventConsumer eventConsumer, String dataset, boolean softDelete)
    throws Exception {
    List<String> tableNames = Arrays.asList("users1", "users2", "users3");

    long sequenceNum = createDatasetAndTable(eventConsumer, dataset, tableNames);

    /*
        send events:
          insert <0, 'alice', 0L, 1970-01-01, 0.0>
          insert <1, 'bob', 86400L, 1970-01-02, 1.0>
          update id=0 to id=2
          delete id=1 (bob)

        should result in:
          2, 'alice', 1000L, 1970-01-01, 0.0
     */
    StructuredRecord insert1 = StructuredRecord.builder(USER_SCHEMA)
      .set("id", 0)
      .set("name", "alice")
      .setTimestamp("created", ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")))
      .setDate("bday", LocalDate.ofEpochDay(0))
      .set("score", 0.0d)
      .set("partition", 1)
      .build();
    for (String tableName : tableNames) {
      DMLEvent insert1Event = DMLEvent.builder()
        .setOperationType(DMLOperation.Type.INSERT)
        .setIngestTimestamp(0L)
        .setSnapshot(false)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setRow(insert1)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDML(new Sequenced<>(insert1Event, sequenceNum++));
    }

    StructuredRecord insert2 = StructuredRecord.builder(USER_SCHEMA)
      .set("id", 1)
      .set("name", "bob")
      .setTimestamp("created", ZonedDateTime.ofInstant(Instant.ofEpochSecond(86400), ZoneId.of("UTC")))
      .setDate("bday", LocalDate.ofEpochDay(1))
      .set("score", 1.0d)
      .build();
    for (String tableName : tableNames) {
      DMLEvent insert2Event = DMLEvent.builder()
        .setOperationType(DMLOperation.Type.INSERT)
        .setIngestTimestamp(1L)
        .setSnapshot(false)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setRow(insert2)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDML(new Sequenced<>(insert2Event, sequenceNum++));
    }
    eventConsumer.flush();

    for (String tableName : tableNames) {
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
      Assert.assertEquals(1, row.get("partition").getLongValue());
      row = iter.next();
      Assert.assertEquals(1L, row.get("id").getLongValue());
      Assert.assertEquals("bob", row.get("name").getStringValue());
      Assert.assertEquals(TimeUnit.SECONDS.toMicros(86400), row.get("created").getTimestampValue());
      Assert.assertEquals("1970-01-02", row.get("bday").getStringValue());
      Assert.assertEquals(1.0d, row.get("score").getDoubleValue(), 0.000001d);
      Assert.assertTrue(row.get("partition").isNull());
      // staging table should be cleaned up
      Assert.assertNull(bigQuery.getTable(TableId.of(dataset, STAGING_TABLE_PREFIX + tableName)));
    }

    StructuredRecord update = StructuredRecord.builder(USER_SCHEMA)
      .set("id", 2)
      .set("name", insert1.get("name"))
      .setTimestamp("created", insert1.getTimestamp("created"))
      .setDate("bday", insert1.getDate("bday"))
      .set("score", insert1.get("score"))
      .set("partition", insert1.get("partition"))
      .build();
    for (String tableName : tableNames) {
      DMLEvent updateEvent = DMLEvent.builder()
        .setOperationType(DMLOperation.Type.UPDATE)
        .setIngestTimestamp(2L)
        .setSnapshot(false)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setPreviousRow(insert1)
        .setRow(update)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDML(new Sequenced<>(updateEvent, sequenceNum++));

      DMLEvent deleteEvent = DMLEvent.builder()
        .setOperationType(DMLOperation.Type.DELETE)
        .setIngestTimestamp(3L)
        .setSnapshot(false)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setRow(insert2)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDML(new Sequenced<>(deleteEvent, sequenceNum++));
    }
    eventConsumer.flush();

    for (String tableName : tableNames) {
      // should have just one row: 0, 'Alice', 1000L, 1970-01-01, 0.0
      TableResult result = executeQuery(String.format("SELECT * from %s.%s", dataset, tableName));

      long expectedRows = softDelete ? 2L : 1L;
      Assert.assertEquals(expectedRows, result.getTotalRows());

      Set<String> expectedNames = ImmutableSet.of("alice", "bob");

      for (FieldValueList row : result.iterateAll()) {
        String name = row.get("name").getStringValue();
        Assert.assertTrue(expectedNames.contains(name));
        if (!softDelete) {
          Assert.assertEquals("alice", name);
        }

        if (name.equals("alice")) {
          Assert.assertEquals(2L, row.get("id").getLongValue());
          Assert.assertEquals("alice", row.get("name").getStringValue());
          Assert.assertEquals(0L, row.get("created").getTimestampValue());
          Assert.assertEquals("1970-01-01", row.get("bday").getStringValue());
          Assert.assertEquals(0.0d, row.get("score").getDoubleValue(), 0.000001d);
          Assert.assertEquals(1L, row.get("partition").getLongValue());
        } else if (name.equals("bob")) {
          Assert.assertEquals(1L, row.get("id").getLongValue());
          Assert.assertEquals("bob", row.get("name").getStringValue());
          Assert.assertEquals("1970-01-02", row.get("bday").getStringValue());
          Assert.assertEquals(1.0d, row.get("score").getDoubleValue(), 0.000001d);
          Assert.assertTrue(row.get("_is_deleted").getBooleanValue());
        } else {
          Assert.fail("Name in the record should either be 'alice' or 'bob'.");
        }
      }
      // staging table should be cleaned up
      Assert.assertNull(bigQuery.getTable(TableId.of(dataset, STAGING_TABLE_PREFIX + tableName)));
    }
  }

  private void insertTruncate(BigQueryEventConsumer eventConsumer, String dataset) throws Exception {
    List<String> tableNames = Arrays.asList("users1", "users2", "users3");

    long sequenceNum = createDatasetAndTable(eventConsumer, dataset, tableNames);

    /*
        send events:
          insert <0, 'alice', 0L, 1970-01-01, 0.0>
          insert <1, 'bob', 86400L, 1970-01-02, 1.0>
          truncate

        should result in empty records in the table
     */
    StructuredRecord insert1 = StructuredRecord.builder(USER_SCHEMA)
      .set("id", 0)
      .set("name", "alice")
      .setTimestamp("created", ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.of("UTC")))
      .setDate("bday", LocalDate.ofEpochDay(0))
      .set("score", 0.0d)
      .build();
    for (String tableName : tableNames) {
      DMLEvent insert1Event = DMLEvent.builder()
        .setOperationType(DMLOperation.Type.INSERT)
        .setIngestTimestamp(0L)
        .setSnapshot(false)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setRow(insert1)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDML(new Sequenced<>(insert1Event, sequenceNum++));
    }

    StructuredRecord insert2 = StructuredRecord.builder(USER_SCHEMA)
      .set("id", 1)
      .set("name", "bob")
      .setTimestamp("created", ZonedDateTime.ofInstant(Instant.ofEpochSecond(86400), ZoneId.of("UTC")))
      .setDate("bday", LocalDate.ofEpochDay(1))
      .set("score", 1.0d)
      .build();
    for (String tableName : tableNames) {
      DMLEvent insert2Event = DMLEvent.builder()
        .setOperationType(DMLOperation.Type.INSERT)
        .setIngestTimestamp(1L)
        .setSnapshot(false)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setRow(insert2)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDML(new Sequenced<>(insert2Event, sequenceNum++));
    }
    eventConsumer.flush();

    for (String tableName : tableNames) {
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
      // staging table should be cleaned up
      Assert.assertNull(bigQuery.getTable(TableId.of(dataset, STAGING_TABLE_PREFIX + tableName)));
    }

    for (String tableName : tableNames) {
      DDLEvent truncateEvent = DDLEvent.builder()
        .setOperation(DDLOperation.Type.TRUNCATE_TABLE)
        .setSnapshot(false)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDDL(new Sequenced<>(truncateEvent, sequenceNum++));
    }

    for (String tableName : tableNames) {
      // should have 0 row
      TableResult result = executeQuery(String.format("SELECT * from %s.%s", dataset, tableName));
      Assert.assertEquals(0L, result.getTotalRows());
    }
  }

  private long createDatasetAndTable(BigQueryEventConsumer eventConsumer, String dataset,
                                     List<String> tableNames) throws Exception {
    long sequenceNum = 1L;
    // test creation of dataset
    DDLEvent createDatabase = DDLEvent.builder()
      .setOperation(DDLOperation.Type.CREATE_DATABASE)
      .setDatabaseName(dataset)
      .build();

    eventConsumer.applyDDL(new Sequenced<>(createDatabase, sequenceNum++));
    Dataset ds = bigQuery.getDataset(dataset);
    Assert.assertNotNull(ds);

    // test creation of tables
    for (String tableName : tableNames) {
      DDLEvent createEvent = DDLEvent.builder()
        .setOperation(DDLOperation.Type.CREATE_TABLE)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setSchema(USER_SCHEMA)
        .setPrimaryKey(Collections.singletonList("id"))
        .build();
      eventConsumer.applyDDL(new Sequenced<>(createEvent, sequenceNum++));

      Table table = bigQuery.getTable(TableId.of(dataset, tableName));
      Assert.assertNotNull(table);
      TableDefinition tableDefinition = table.getDefinition();
      FieldList bqFields = tableDefinition.getSchema().getFields();
      Assert.assertEquals(LegacySQLTypeName.INTEGER, bqFields.get("id").getType());
      Assert.assertEquals(LegacySQLTypeName.STRING, bqFields.get("name").getType());
      Assert.assertEquals(LegacySQLTypeName.TIMESTAMP, bqFields.get("created").getType());
      Assert.assertEquals(LegacySQLTypeName.DATE, bqFields.get("bday").getType());
      Assert.assertEquals(LegacySQLTypeName.FLOAT, bqFields.get("score").getType());
    }

    return sequenceNum;
  }

  private TableResult executeQuery(String query) throws InterruptedException {
    QueryJobConfiguration jobConfig = QueryJobConfiguration.of(query);
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = BigQueryUtils.createBigQueryJob(bigQuery, JobInfo.newBuilder(jobConfig).setJobId(jobId).build());
    queryJob.waitFor();
    return queryJob.getQueryResults();
  }

  private void cleanupTest(Bucket bucket, String dataset, BigQueryEventConsumer eventConsumer) {
    for (Blob blob : bucket.list().iterateAll()) {
      storage.delete(blob.getBlobId());
    }
    bucket.delete();
    Dataset ds = bigQuery.getDataset(dataset);
    if (ds != null) {
      ds.delete(BigQuery.DatasetDeleteOption.deleteContents());
    }
    eventConsumer.stop();
  }
}
