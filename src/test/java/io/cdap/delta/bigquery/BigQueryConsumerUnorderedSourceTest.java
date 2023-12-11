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
import org.junit.Before;
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
  public static final String ID_PRIMARY_KEY = "id";
  private static final Schema USER_SCHEMA = Schema.recordOf("user",
          Schema.Field.of(ID_PRIMARY_KEY, Schema.of(Schema.Type.INT)),
          Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("score", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))));

  private static Storage storage;
  private static BigQuery bigQuery;
  private static String project;
  private static DeltaTargetContext context;
  private Bucket bucket;

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

  @Before
  public void setupTest() {
    String bucketName = "bqtest-" + UUID.randomUUID();
    bucket = storage.create(BucketInfo.of(bucketName));
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
    String dataset = "unordered_scenario_1";
    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(context, storage, bigQuery, bucket,
            project, 1, STAGING_TABLE_PREFIX, false, null, null,
            dataset, false, false);
    List<String> tableNames = Arrays.asList("users1", "users2");
    try {
      long sequenceNum = createDatasetAndTable(eventConsumer, dataset, tableNames);

      List<SortKey> sortKeys = createSortKeys(1000L, 100L, "dummy1");
      List<SortKey> sortKeysOlder = createSortKeys(1000L, 90L, "dummy1");
      List<SortKey> sortKeysNewer = createSortKeys(1000L, 110L, "dummy1");
      List<SortKey> sortKeysLatest = createSortKeys(1000L, 110L, "dummy2");

      List<StructuredRecord> records = new ArrayList<>();
      records.add(createRecord(0, "alice", 0.0d));
      records.add(createRecord(1, "bob", 1.0d));

      for (String tableName : tableNames) {
        long ingestTimestamp = 0L;
        for (StructuredRecord record : records) {
          DMLEvent insertEvent = DMLEvent.builder()
                  .setOperationType(DMLOperation.Type.INSERT)
                  .setIngestTimestamp(ingestTimestamp++)
                  .setSnapshot(true)
                  .setDatabaseName(dataset)
                  .setTableName(tableName)
                  .setRow(record)
                  .setSourceTimestamp(sortKeys.get(0).getValue())
                  .setSortKeys(sortKeys)
                  .setOffset(new Offset())
                  .build();

          eventConsumer.applyDML(new Sequenced<>(insertEvent, sequenceNum++));
        }
      }
      eventConsumer.flush();

      for (String tableName : tableNames) {
        TableResult result = queryAllRows(dataset, tableName);
        Assert.assertEquals(records.size(), result.getTotalRows());
        Iterator<FieldValueList> iter = result.iterateAll().iterator();
        FieldValueList row = iter.next();
        validateRow(row, 0L, "alice", 0.0d, sortKeys, false);
        row = iter.next();
        validateRow(row, 1L, "bob", 1.0d, sortKeys, false);
      }

      StructuredRecord update1 = createRecord(0, "alice", 2.0d);
      StructuredRecord update2 = createRecord(1, "bob", 2.0d);

      for (String tableName : tableNames) {
        // For id=0, name=alice, generate an update event with older sort key which should be dropped
        DMLEvent updateEvent = DMLEvent.builder()
                .setOperationType(DMLOperation.Type.UPDATE)
                .setIngestTimestamp(2L)
                .setSnapshot(false)
                .setDatabaseName(dataset)
                .setTableName(tableName)
                .setPreviousRow(records.get(0))
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
                .setPreviousRow(records.get(1))
                .setRow(update2)
                .setSourceTimestamp(sortKeysNewer.get(0).getValue())
                .setSortKeys(sortKeysNewer)
                .setOffset(new Offset())
                .build();
        eventConsumer.applyDML(new Sequenced<>(updateEvent2, sequenceNum++));
      }
      eventConsumer.flush();

      for (String tableName : tableNames) {
        TableResult result = queryAllRows(dataset, tableName);

        Assert.assertEquals(2L, result.getTotalRows());
        Iterator<FieldValueList> iter = result.iterateAll().iterator();
        FieldValueList row = iter.next();
        // no change as event with sortKeysOlder should be dropped
        validateRow(row, 0L, "alice", 0.0d, sortKeys, false);

        row = iter.next();
        // delete event with sortKeysLatest should be merged
        validateRow(row, 1L, "bob", 2.0d, sortKeysLatest, true);
      }
    } finally {
      BigQueryTestUtils.cleanupTest(bucket, dataset, eventConsumer, bigQuery, storage);
    }
  }

  /**
   * Test verifies that BigQueryEventConsumer correctly implements ordering for DML events
   * based on sort keys when CDC events are received directly without any snapshot events
   * <p>
   * 1. Generate DDL
   * 2. Generate Update/Delete CDC events with older/newer sort keys
   * 3. Verify that only CDC events with newer sort keys are merged to target table
   *
   * @throws Exception
   */
  @Test
  public void testConcurrentUpdateEventsWithoutSnapshot() throws Exception {
    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(context, storage, bigQuery, bucket,
            project, 1, STAGING_TABLE_PREFIX, false, null, null,
            null, false, false);
    String dataset = "unordered_scenario_2";
    List<String> tableNames = Arrays.asList("users1", "users2");
    try {
      long sequenceNum = createDatasetAndTable(eventConsumer, dataset, tableNames);

      List<SortKey> sortKeys = createSortKeys(1000L, 100L, "dummy1");
      List<SortKey> sortKeysNewer = createSortKeys(1000L, 110L, "dummy1");
      List<SortKey> sortKeysLatest = createSortKeys(1000L, 110L, "dummy2");

      StructuredRecord update1 = createRecord(0, "alice", 2.0d);
      StructuredRecord update2 = createRecord(1, "bob", 2.0d);
      StructuredRecord insert1 = createRecord(2, "james", 3.0d);

      for (String tableName : tableNames) {
        // For id=0, name=alice, generate an update event with older sort key which should be dropped
        DMLEvent updateEvent = DMLEvent.builder()
                .setOperationType(DMLOperation.Type.UPDATE)
                .setIngestTimestamp(2L)
                .setSnapshot(false)
                .setDatabaseName(dataset)
                .setTableName(tableName)
                .setPreviousRow(update1)
                .setRow(update1)
                .setSourceTimestamp(sortKeys.get(0).getValue())
                .setSortKeys(sortKeys)
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
                .setPreviousRow(update2)
                .setRow(update2)
                .setSourceTimestamp(sortKeysNewer.get(0).getValue())
                .setSortKeys(sortKeysNewer)
                .setOffset(new Offset())
                .build();
        eventConsumer.applyDML(new Sequenced<>(updateEvent2, sequenceNum++));

        DMLEvent insertEvent1 = DMLEvent.builder()
                .setOperationType(DMLOperation.Type.INSERT)
                .setIngestTimestamp(3L)
                .setSnapshot(false)
                .setDatabaseName(dataset)
                .setTableName(tableName)
                .setRow(insert1)
                .setSourceTimestamp(sortKeysNewer.get(0).getValue())
                .setSortKeys(sortKeysNewer)
                .setOffset(new Offset())
                .build();
        eventConsumer.applyDML(new Sequenced<>(insertEvent1, sequenceNum++));
      }
      eventConsumer.flush();

      for (String tableName : tableNames) {
        TableResult result = queryAllRows(dataset, tableName);

        Assert.assertEquals(3L, result.getTotalRows());
        Iterator<FieldValueList> iter = result.iterateAll().iterator();
        FieldValueList row = iter.next();
        // no change as event with sortKeysOlder should be dropped
        validateRow(row, 0L, "alice", 2.0d, sortKeys, false);

        row = iter.next();
        // delete event with sortKeysLatest should be merged
        validateRow(row, 1L, "bob", 2.0d, sortKeysLatest, true);

        row = iter.next();
        // insert event with sortKeysNewer should be merged
        validateRow(row, 2L, "james", 3.0d, sortKeysNewer, false);
      }
    } finally {
      BigQueryTestUtils.cleanupTest(bucket, dataset, eventConsumer, bigQuery, storage);
    }
  }

  /**
   * Test verifies that BigQueryEventConsumer correctly implements ordering for DML events
   * based on sort keys metadata field while also ensuring that sort key column is not deleted
   * while handling Alter table events
   * <p>
   * 1. Generate DDL and snapshot events
   * 2. Generate Alter table event
   * 2. Generate Update/Delete CDC events with older/newer sort keys
   * 3. Verify that only CDC events with newer sort keys are merged to target table
   *
   * @throws Exception
   */
  @Test
  public void testConcurrentUpdatesWithAlterEvent() throws Exception {
    String dataset = "unordered_scenario_3";
    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(context, storage, bigQuery, bucket,
            project, 0, STAGING_TABLE_PREFIX, false, null, null,
            dataset, false, false);
    List<String> tableNames = Arrays.asList("users1", "users2");
    try {
      long sequenceNum = createDatasetAndTable(eventConsumer, dataset, tableNames);

      List<SortKey> sortKeys = createSortKeys(1000L, 100L, "dummy1");
      List<SortKey> sortKeysOlder = createSortKeys(1000L, 90L, "dummy1");
      List<SortKey> sortKeysNewer = createSortKeys(1000L, 110L, "dummy1");
      List<SortKey> sortKeysLatest = createSortKeys(1000L, 110L, "dummy2");

      List<StructuredRecord> records = new ArrayList<>();
      records.add(createRecord(0, "alice", 0.0d));
      records.add(createRecord(1, "bob", 1.0d));

      for (String tableName : tableNames) {
        long ingestTimestamp = 0L;
        for (StructuredRecord record : records) {
          DMLEvent insertEvent = DMLEvent.builder()
                  .setOperationType(DMLOperation.Type.INSERT)
                  .setIngestTimestamp(ingestTimestamp++)
                  .setSnapshot(true)
                  .setDatabaseName(dataset)
                  .setTableName(tableName)
                  .setRow(record)
                  .setSourceTimestamp(sortKeys.get(0).getValue())
                  .setSortKeys(sortKeys)
                  .setOffset(new Offset())
                  .build();

          eventConsumer.applyDML(new Sequenced<>(insertEvent, sequenceNum++));
        }
      }
      eventConsumer.flush();

      for (String tableName : tableNames) {
        TableResult result = queryAllRows(dataset, tableName);
        Assert.assertEquals(records.size(), result.getTotalRows());
        Iterator<FieldValueList> iter = result.iterateAll().iterator();
        FieldValueList row = iter.next();
        validateRow(row, 0L, "alice", 0.0d, sortKeys, false);
        row = iter.next();
        validateRow(row, 1L, "bob", 1.0d, sortKeys, false);
      }

      Schema updatedSchema =
              Schema.recordOf("user.new",
                      Schema.Field.of(ID_PRIMARY_KEY, Schema.of(Schema.Type.INT)),
                      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("score", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                      // add a new nullable age field
                      Schema.Field.of("age", Schema.nullableOf(Schema.of(Schema.Type.INT))));
      for (String tableName : tableNames) {
        DDLEvent alterEvent = DDLEvent.builder()
                .setOperation(DDLOperation.Type.ALTER_TABLE)
                .setDatabaseName(dataset)
                .setTableName(tableName)
                .setSchema(updatedSchema)
                .setPrimaryKey(Collections.singletonList(ID_PRIMARY_KEY))
                .setOffset(new Offset())
                .setSnapshot(false)
                .build();
        eventConsumer.applyDDL(new Sequenced<>(alterEvent, sequenceNum + 1));
      }

      StructuredRecord update1 = createRecord(0, "alice", 2.0d, null, updatedSchema);
      StructuredRecord update2 = createRecord(1, "bob", 2.0d, 20, updatedSchema);

      for (String tableName : tableNames) {
        // For id=0, name=alice, generate an update event with older sort key which should be dropped
        DMLEvent updateEvent = DMLEvent.builder()
                .setOperationType(DMLOperation.Type.UPDATE)
                .setIngestTimestamp(2L)
                .setSnapshot(false)
                .setDatabaseName(dataset)
                .setTableName(tableName)
                .setPreviousRow(records.get(0))
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
                .setPreviousRow(records.get(1))
                .setRow(update2)
                .setSourceTimestamp(sortKeysNewer.get(0).getValue())
                .setSortKeys(sortKeysNewer)
                .setOffset(new Offset())
                .build();
        eventConsumer.applyDML(new Sequenced<>(updateEvent2, sequenceNum++));
      }
      eventConsumer.flush();

      for (String tableName : tableNames) {
        TableResult result = queryAllRows(dataset, tableName);

        Assert.assertEquals(2L, result.getTotalRows());
        Iterator<FieldValueList> iter = result.iterateAll().iterator();
        FieldValueList row = iter.next();
        // no change as event with sortKeysOlder should be dropped
        validateRow(row, 0L, "alice", 0.0d, null, sortKeys, false);

        row = iter.next();
        // delete event with sortKeysLatest should be merged
        validateRow(row, 1L, "bob", 2.0d, 20L, sortKeysLatest, true);
      }
    } finally {
      BigQueryTestUtils.cleanupTest(bucket, dataset, eventConsumer, bigQuery, storage);
    }
  }

  private long createDatasetAndTable(BigQueryEventConsumer eventConsumer, String dataset,
                                     List<String> tableNames) throws Exception {
    long sequenceNum = 1L;
    DDLEvent createDatabase = DDLEvent.builder()
            .setOperation(DDLOperation.Type.CREATE_DATABASE)
            .setDatabaseName(dataset)
            .build();

    eventConsumer.applyDDL(new Sequenced<>(createDatabase, sequenceNum++));
    Dataset ds = bigQuery.getDataset(dataset);
    Assert.assertNotNull(ds);

    for (String tableName : tableNames) {
      DDLEvent createEvent = DDLEvent.builder()
              .setOperation(DDLOperation.Type.CREATE_TABLE)
              .setDatabaseName(dataset)
              .setTableName(tableName)
              .setSchema(USER_SCHEMA)
              .setPrimaryKey(Collections.singletonList(ID_PRIMARY_KEY))
              .build();
      eventConsumer.applyDDL(new Sequenced<>(createEvent, sequenceNum++));
      Table table = bigQuery.getTable(TableId.of(dataset, tableName));
      Assert.assertNotNull(table);
    }
    return sequenceNum;
  }

  private List<SortKey> createSortKeys(long key1, long key2, String key3) {
    List<SortKey> sortKeys = new ArrayList<>();
    sortKeys.add(new SortKey(Schema.Type.LONG, key1));
    sortKeys.add(new SortKey(Schema.Type.LONG, key2));
    sortKeys.add(new SortKey(Schema.Type.STRING, key3));
    return sortKeys;
  }

  private StructuredRecord createRecord(int id, String name, double score) {
    return StructuredRecord.builder(USER_SCHEMA)
            .set(ID_PRIMARY_KEY, id)
            .set("name", name)
            .set("score", score)
            .build();
  }

  private StructuredRecord createRecord(int id, String name, double score, Integer age) {
    return createRecord(id, name, score, age, USER_SCHEMA);
  }

  private StructuredRecord createRecord(int id, String name, double score, Integer age, Schema schema) {
    return StructuredRecord.builder(schema)
            .set(ID_PRIMARY_KEY, id)
            .set("name", name)
            .set("score", score)
            .set("age", age)
            .build();
  }

  private void validateRow(FieldValueList row, long id, String name, double score, List<SortKey> sortKeys,
                           boolean isDeleted) {
    Assert.assertEquals(id, row.get(ID_PRIMARY_KEY).getLongValue());
    Assert.assertEquals(name, row.get("name").getStringValue());
    Assert.assertEquals(score, row.get("score").getDoubleValue(), 0.000001d);
    if (isDeleted) {
      Assert.assertTrue(row.get("_is_deleted").getBooleanValue());
    } else {
      Assert.assertTrue(row.get("_is_deleted").getValue() == null ||
              row.get("_is_deleted").getBooleanValue() == Boolean.FALSE);
    }

    validateSortKeyInRow(row, sortKeys);
  }

  private void validateRow(FieldValueList row, long id, String name, double score, Long age, List<SortKey> sortKeys,
                           boolean isDeleted) {
    validateRow(row, id, name, score, sortKeys, isDeleted);
    if (age == null) {
      Assert.assertTrue(row.get("age").getValue() == null);
    } else {
      Assert.assertEquals(age.longValue(), row.get("age").getLongValue());
    }
  }

  private void validateSortKeyInRow(FieldValueList row, List<SortKey> sortKeys) {
    FieldValueList sort = row.get("_sort").getRecordValue();
    Assert.assertEquals((long) sortKeys.get(0).getValue(), sort.get("_key_0").getLongValue());
    Assert.assertEquals((long) sortKeys.get(1).getValue(), sort.get("_key_1").getLongValue());
    Assert.assertEquals(sortKeys.get(2).getValue(), sort.get("_key_2").getStringValue());
  }

  private TableResult queryAllRows(String dataset, String tableName) throws InterruptedException {
    return BigQueryTestUtils.executeQuery(
            String.format("SELECT * from %s.%s order by id", dataset, tableName), bigQuery);
  }
}
