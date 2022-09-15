/*
 * Copyright © 2022 Cask Data, Inc.
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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
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
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.Sequenced;
import io.cdap.delta.api.SortKey;
import io.cdap.delta.api.SourceProperties;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for BigQueryEventConsumer for unordered source streams.
 * In order to run these tests, service account credentials must be set in the system
 * properties. The service account must have permission to create and write to BigQuery datasets and tables,
 * as well as permission to write to GCS.
 *
 * The tests create real resources in GCP and will cost some small amount of money for each run.
 */
public class BigQueryConsumerUnorderedSourceTest {
  private static final String STAGING_TABLE_PREFIX = "_staging_";
  private static final Schema USER_SCHEMA = Schema.recordOf("user",
    Schema.Field.of("id", Schema.of(Schema.Type.INT)),
    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("score", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

  private static Storage storage;
  private static BigQuery bigQuery;
  private static String project;
  private static DeltaTargetContext context;

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

    Map<String, String> runtimeArguments = new HashMap<>();
    runtimeArguments.put("gcp.bigquery.max.clustering.columns", "4");

    context = mock(DeltaTargetContext.class);
    when(context.getRuntimeArguments()).thenReturn(runtimeArguments);
    when(context.getMaxRetrySeconds()).thenReturn(300);
    when(context.getAllTables()).thenReturn(Collections.emptySet());
    SourceProperties sourceProperties = mock(SourceProperties.class);
    when(sourceProperties.getOrdering()).thenReturn(SourceProperties.Ordering.UN_ORDERED);
    when(context.getSourceProperties()).thenReturn(sourceProperties);
  }

  /**
   * Test verifies that BigQueryEventConsumer correctly implements ordering for DML events
   * based on sort keys metadata field while merging data to target BQ table
   *
   * 1. Generate DDL and snapshot events
   * 2. Generate Update/Delete CDC events with older/newer sort keys
   * 3. Verify that only CDC events with newer sort keys are merged to target table
   *
   * @throws Exception
   */
  @Test
  public void testSnapshotAndConcurrentUpdateEvents() throws Exception {
    String bucketName = "bqtest-" + UUID.randomUUID().toString();
    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(context, storage, bigQuery, bucket,
                                                                    project, 1, STAGING_TABLE_PREFIX, false, null, null,
                                                                    null, false);
    String dataset = "unordered_scenario_1";
    try {
      testConcurrentUpdates(eventConsumer, dataset, false);
    } finally {
      BigQueryTestUtils.cleanupTest(bucket, dataset, eventConsumer, bigQuery, storage);
    }
  }


  private void testConcurrentUpdates(BigQueryEventConsumer eventConsumer, String dataset, boolean softDelete)
    throws Exception {
    List<String> tableNames = Arrays.asList("users1", "users2");

    long sequenceNum = createDatasetAndTable(eventConsumer, dataset, tableNames);

    List<SortKey> sortKeys = new ArrayList<>();
    sortKeys.add(new SortKey(Schema.Type.LONG, 1000L));
    sortKeys.add(new SortKey(Schema.Type.LONG, 100L));
    sortKeys.add(new SortKey(Schema.Type.STRING, "dummy1"));

    List<SortKey> sortKeysOlder = new ArrayList<>();
    sortKeysOlder.add(new SortKey(Schema.Type.LONG, 1000L));
    sortKeysOlder.add(new SortKey(Schema.Type.LONG, 90L));
    sortKeysOlder.add(new SortKey(Schema.Type.STRING, "dummy1"));

    List<SortKey> sortKeysNewer = new ArrayList<>();
    sortKeysNewer.add(new SortKey(Schema.Type.LONG, 1000L));
    sortKeysNewer.add(new SortKey(Schema.Type.LONG, 110L));
    sortKeysNewer.add(new SortKey(Schema.Type.STRING, "dummy1"));

    List<SortKey> sortKeysLatest = new ArrayList<>();
    sortKeysLatest.add(new SortKey(Schema.Type.LONG, 1000L));
    sortKeysLatest.add(new SortKey(Schema.Type.LONG, 110L));
    sortKeysLatest.add(new SortKey(Schema.Type.STRING, "dummy2"));

    StructuredRecord insert1 = StructuredRecord.builder(USER_SCHEMA)
            .set("id", 0)
            .set("name", "alice")
            .set("score", 0.0d)
            .build();
    for (String tableName : tableNames) {
      DMLEvent insert1Event = DMLEvent.builder()
              .setOperationType(DMLOperation.Type.INSERT)
              .setIngestTimestamp(0L)
        .setSnapshot(true)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setRow(insert1)
        .setSourceTimestamp(sortKeys.get(0).getValue())
        .setSortKeys(sortKeys)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDML(new Sequenced<>(insert1Event, sequenceNum++));
    }

    StructuredRecord insert2 = StructuredRecord.builder(USER_SCHEMA)
      .set("id", 1)
      .set("name", "bob")
      .set("score", 1.0d)
      .build();
    for (String tableName : tableNames) {
      DMLEvent insert2Event = DMLEvent.builder()
        .setOperationType(DMLOperation.Type.INSERT)
        .setIngestTimestamp(1L)
        .setSnapshot(true)
        .setDatabaseName(dataset)
        .setTableName(tableName)
        .setRow(insert2)
        .setSourceTimestamp(sortKeys.get(0).getValue())
        .setSortKeys(sortKeys)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDML(new Sequenced<>(insert2Event, sequenceNum++));
    }
    eventConsumer.flush();

    for (String tableName : tableNames) {
      TableResult result = BigQueryTestUtils.executeQuery(
              String.format("SELECT * from %s.%s order by id", dataset, tableName), bigQuery);
      Assert.assertEquals(2, result.getTotalRows());
      Iterator<FieldValueList> iter = result.iterateAll().iterator();
      FieldValueList row = iter.next();
      Assert.assertEquals(0L, row.get("id").getLongValue());
      Assert.assertEquals("alice", row.get("name").getStringValue());
      Assert.assertEquals(0.0d, row.get("score").getDoubleValue(), 0.000001d);
      validateSortKeyInRow(row, sortKeys);

      row = iter.next();
      Assert.assertEquals(1L, row.get("id").getLongValue());
      Assert.assertEquals("bob", row.get("name").getStringValue());
      Assert.assertEquals(1.0d, row.get("score").getDoubleValue(), 0.000001d);
      validateSortKeyInRow(row, sortKeys);
    }

    StructuredRecord update1 = StructuredRecord.builder(USER_SCHEMA)
      .set("id", 0)
      .set("name", insert1.get("name"))
      .set("score", 2.0)
      .build();

    StructuredRecord update2 = StructuredRecord.builder(USER_SCHEMA)
            .set("id", 1)
            .set("name", insert2.get("name"))
            .set("score", 2.0)
            .build();
    for (String tableName : tableNames) {
      // For id=0, name=alice, generate an update event with older sort key which should be dropped
      DMLEvent updateEvent = DMLEvent.builder()
              .setOperationType(DMLOperation.Type.UPDATE)
              .setIngestTimestamp(2L)
              .setSnapshot(false)
              .setDatabaseName(dataset)
              .setTableName(tableName)
              .setPreviousRow(insert1)
              .setRow(update1)
              .setSourceTimestamp(sortKeysOlder.get(0).getValue())
              .setSortKeys(sortKeysOlder)
              .setOffset(new Offset())
              .build();
      eventConsumer.applyDML(new Sequenced<>(updateEvent, sequenceNum++));

      // For id=1, name=bob, generate 2 events
      // 1. delete event with sortKeysLatest which should be merged
      // 2. update event with sortKeysNewer which should be dropped as it's older than sortKeysLatest
      DMLEvent deleteEvent = DMLEvent.builder()
              .setOperationType(DMLOperation.Type.DELETE)
              .setIngestTimestamp(2L)
              .setSnapshot(false)
              .setDatabaseName(dataset)
              .setTableName(tableName)
              .setRow(update2)
              .setSourceTimestamp(sortKeysLatest.get(0).getValue())
              .setSortKeys(sortKeysLatest)
              .setOffset(new Offset())
              .build();
      eventConsumer.applyDML(new Sequenced<>(deleteEvent, sequenceNum++));

      DMLEvent updateEvent2 = DMLEvent.builder()
              .setOperationType(DMLOperation.Type.UPDATE)
              .setIngestTimestamp(3L)
              .setSnapshot(false)
              .setDatabaseName(dataset)
              .setTableName(tableName)
              .setPreviousRow(insert2)
              .setRow(update2)
              .setSourceTimestamp(sortKeysNewer.get(0).getValue())
              .setSortKeys(sortKeysNewer)
              .setOffset(new Offset())
              .build();
      eventConsumer.applyDML(new Sequenced<>(updateEvent2, sequenceNum++));
    }
    eventConsumer.flush();

    for (String tableName : tableNames) {
      TableResult result = BigQueryTestUtils.executeQuery(
              String.format("SELECT * from %s.%s", dataset, tableName), bigQuery);

      long expectedRows = 2L;
      Assert.assertEquals(expectedRows, result.getTotalRows());

      Set<String> expectedNames = ImmutableSet.of("alice", "bob");

      for (FieldValueList row : result.iterateAll()) {
        String name = row.get("name").getStringValue();
        Assert.assertTrue(expectedNames.contains(name));
        if (name.equals("alice")) {
          // no change as event with sortKeysOlder should be dropped
          Assert.assertEquals(0, row.get("id").getLongValue());
          Assert.assertEquals("alice", row.get("name").getStringValue());
          Assert.assertEquals(0.0d, row.get("score").getDoubleValue(), 0.000001d);
          validateSortKeyInRow(row, sortKeys);
        } else if (name.equals("bob")) {
          // delete event with sortKeysLatest should be merged
          Assert.assertEquals(1L, row.get("id").getLongValue());
          Assert.assertEquals("bob", row.get("name").getStringValue());
          Assert.assertEquals(2.0d, row.get("score").getDoubleValue(), 0.000001d);
          Assert.assertTrue(row.get("_is_deleted").getBooleanValue());
          validateSortKeyInRow(row, sortKeysLatest);
        } else {
          Assert.fail("Name in the record should either be 'alice' or 'bob'.");
        }
      }
    }
  }

  private void validateSortKeyInRow(FieldValueList row, List<SortKey> sortKeys) {
    FieldValueList sort = row.get("_sort").getRecordValue();
    Assert.assertEquals((long) sortKeys.get(0).getValue(), sort.get("_key_0").getLongValue());
    Assert.assertEquals((long) sortKeys.get(1).getValue(), sort.get("_key_1").getLongValue());
    Assert.assertEquals((String) sortKeys.get(2).getValue(), sort.get("_key_2").getStringValue());
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
      Assert.assertEquals(LegacySQLTypeName.FLOAT, bqFields.get("score").getType());
    }
    return sequenceNum;
  }
}
