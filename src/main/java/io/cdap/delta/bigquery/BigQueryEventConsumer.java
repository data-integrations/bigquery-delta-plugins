/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.Sequenced;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.TimeoutExceededException;
import net.jodah.failsafe.function.ContextualRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Consumes change events and applies them to BigQuery.
 *
 * Writes to BigQuery in three steps.
 *
 * Step 1 - Write a batch of changes to GCS
 *
 * Each batch of changes is written to GCS as an object with path:
 *
 *   [staging bucket]/cdap/cdc/[app name]/[table id]/[batch id]
 *
 * Batch id is the timestamp that the first event in the batch was processed.
 * The size of the batch is determined through configuration.
 * There is a maximum number of rows to include in each batch and a maximum amount of time to wait in between batches.
 * Each object is written in avro format and contains the columns in the destination table plus two additional columns:
 *   _op: CREATE | UPDATE | DELETE
 *   _batch_id: the batch id
 *
 * Changes in the batch do not span across a DDL event, so they are guaranteed to conform to the same schema.
 * Failure scenarios are:
 *
 *   1. The program dies after the object is written, but before the offset is persisted.
 *      When the program starts up again, events for the batch will be replayed.
 *      The consumer will not know which events are duplicates, so duplicate events will be written out to GCS.
 *      This will not matter because of the behavior of later steps.
 *   2. The program dies before the object is written, which is always before the offset is persisted.
 *      In this case, nothing was ever persisted to GCS and everything behaves as if it was the first time
 *      the events were seen.
 *   3. The write to GCS fails for some reason. For example, permissions were revoked, quota was hit,
 *      there was a temporary outage, etc. In this scenario, the write will be repeatedly retried until it
 *      succeeds. It may need manual intervention to succeed.
 *
 * Step 2 - Load data from GCS into staging BigQuery table
 *
 * This step happens after the offset from Step 1 is successfully persisted. This will load the object
 * into a staging table in BigQuery. The staging table has the same schema as the rows in the GCS object.
 * It is clustered on _batch_id in order to make reads and deletes on the _batch_id efficient.
 * The job id for the load is of the form [app name]_stage_[dataset]_[table]_[batch id]_[retry num].
 * Failure scenarios are:
 *
 *   1. The load job fails for some reason. For example, permissions were revoked, quota was hit, temporary outage, etc.
 *      The load will be repeatedly retried until is succeeds. It may need manual intervention to succeed.
 *   2. The program dies. When the program starts up again, events will be replayed from the last committed offset.
 *
 * Step 3 - Merge a batch of data from the staging BigQuery table into the target table
 *
 * This step happens after the load job to the staging table has succeeded. The consumer runs a merge query of the form:
 *
 *   MERGE [dataset].[target table] as T
 *   USING (SELECT * FROM [dataset].[staging table] WHERE _batch_id = [batch id]) as S
 *     ON [row equality condition]
 *   WHEN MATCHED AND S._OP = "DELETE"
 *     DELETE
 *   WHEN MATCHED AND S._OP = "UPDATE"
 *     UPDATE(...)
 *   WHEN NOT MATCHED AND S._OP = "INSERT"
 *     INSERT(...)
 *     VALUES(...)
 *
 * The job id is of the form [app name]_merge_[dataset]_[table]_[batch id]_[retry_num].
 * This query ensures that it does not matter if there are duplicate events in the batch objects on GCS.
 * Duplicate inserts and deletes will not match and be ignored.
 * Duplicate updates will update the target row to be the same that it already is.
 * Once the job succeeds, the corresponding GCS object is deleted and the offset of the latest event is committed.
 * Failure scenarios are:
 *
 *   1. The merge query fails for some reason. The consumer will retry until it succeeds.
 *      It may need manual intervention to succeed.
 *   2. The program dies. Events are replayed from the last committed offset when the program starts back up.
 *   3. The GCS delete fails. The error is logged, but the consumer proceeds on.
 *      Manual deletion of the object is required.
 *
 */
public class BigQueryEventConsumer implements EventConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryEventConsumer.class);
  private static final int MAX_LENGTH = 1024;
  // according to big query dataset and table naming convention, valid name should only contain letters (upper or
  // lower case), numbers, and underscores
  private static final String VALID_NAME_REGEX = "[\\w]+";
  private static final String INVALID_NAME_REGEX = "[^\\w]+";

  private final DeltaTargetContext context;
  private final BigQuery bigQuery;
  private final int batchMaxRows;
  private final int batchMaxSecondsElapsed;
  private final String stagingTablePrefix;
  private final MultiGCSWriter gcsWriter;
  private final Bucket bucket;
  private final String project;
  private final RetryPolicy<Object> commitRetryPolicy;
  private final Map<TableId, Long> latestSeenSequence;
  private final Map<TableId, Long> latestMergedSequence;
  private final Map<TableId, List<String>> primaryKeyStore;
  private ScheduledExecutorService executorService;
  private ScheduledFuture<?> scheduledFlush;
  private Offset latestOffset;
  private long lastFlushTime;
  private long latestSequenceNum;
  private int currentBatchSize;
  private Exception flushException;
  // have to keep all the records in memory in case there is a failure writing to GCS
  // cannot write to a temporary file on local disk either in case there is a failure writing to disk
  // Without keeping the entire batch in memory, there would be no way to recover the records that failed to write

  BigQueryEventConsumer(DeltaTargetContext context, Storage storage, BigQuery bigQuery, Bucket bucket,
                        String project, int batchMaxRows, int batchMaxSecondsElapsed, String stagingTablePrefix) {
    this.context = context;
    this.bigQuery = bigQuery;
    this.batchMaxRows = batchMaxRows;
    this.batchMaxSecondsElapsed = batchMaxSecondsElapsed;
    this.stagingTablePrefix = stagingTablePrefix;
    this.gcsWriter = new MultiGCSWriter(storage, bucket.getName(),
                                        String.format("cdap/delta/%s/", context.getApplicationName()),
                                        context);
    this.bucket = bucket;
    this.project = project;
    this.currentBatchSize = 0;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.latestSequenceNum = 0L;
    this.lastFlushTime = 0L;
    // these maps are only accessed in synchronized methods so they do not need to be thread safe.
    this.latestMergedSequence = new HashMap<>();
    this.latestSeenSequence = new HashMap<>();
    this.primaryKeyStore = new HashMap<>();
    this.commitRetryPolicy = new RetryPolicy<>()
      .withMaxAttempts(Integer.MAX_VALUE)
      .withMaxDuration(Duration.of(5, ChronoUnit.MINUTES))
      .withBackoff(1, 60, ChronoUnit.SECONDS)
      .onFailedAttempt(failureContext -> {
        // log on the first failure and then once per minute
        if (failureContext.getAttemptCount() == 1 || !failureContext.getElapsedTime().minusMinutes(1).isNegative()) {
          LOG.warn("Error committing offset. Changes will be blocked until this succeeds.",
                   failureContext.getLastFailure());
        }
      });
  }

  @Override
  public void start() {
    scheduledFlush = executorService.scheduleAtFixedRate(() -> {
      try {
        flush();
      } catch (InterruptedException e) {
        // just return and let things end
      } catch (Exception e) {
        flushException = e;
      }
    }, batchMaxSecondsElapsed, batchMaxSecondsElapsed, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    scheduledFlush.cancel(true);
    executorService.shutdownNow();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // just return and let everything end
    }
  }

  @Override
  public synchronized void applyDDL(Sequenced<DDLEvent> sequencedEvent) throws Exception {
    // this is non-null if an error happened during a time scheduled flush
    if (flushException != null) {
      throw flushException;
    }

    DDLEvent event = sequencedEvent.getEvent();
    String normalizedDatabaseName = normalize(event.getDatabase());
    String normalizedTableName = normalize(event.getTable());
    switch (event.getOperation()) {
      case CREATE_DATABASE:
        DatasetId datasetId = DatasetId.of(project, normalizedDatabaseName);
        if (bigQuery.getDataset(datasetId) == null) {
          DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).setLocation(bucket.getLocation()).build();
          bigQuery.create(datasetInfo);
        }
        break;
      case DROP_DATABASE:
        datasetId = DatasetId.of(project, normalizedDatabaseName);
        primaryKeyStore.clear();
        if (bigQuery.getDataset(datasetId) != null) {
          bigQuery.delete(datasetId);
        }
        break;
      case CREATE_TABLE:
        TableId tableId = TableId.of(project, normalizedDatabaseName, normalizedTableName);
        Table table = bigQuery.getTable(tableId);
        // TODO: check schema of table if it exists already
        if (table == null) {
          TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(Schemas.convert(addSequenceNumber(event.getSchema())))
            .build();
          List<String> primaryKeys = event.getPrimaryKey();
          if (primaryKeys.isEmpty()) {
            throw new DeltaFailureException(
              String.format("Table '%s' in database '%s' has no primary key. Tables without a primary key are" +
                              " not supported.", tableId.getTable(), tableId.getDataset()));
          }
          primaryKeyStore.put(tableId, primaryKeys);

          TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
          // store in local
          bigQuery.create(tableInfo);
        }
        break;
      case DROP_TABLE:
        // need to flush changes before dropping the table, otherwise the next flush will write data that
        // shouldn't exist
        flush();
        tableId = TableId.of(project, normalizedDatabaseName, normalizedTableName);
        primaryKeyStore.remove(tableId);
        table = bigQuery.getTable(tableId);
        if (table != null) {
          bigQuery.delete(tableId);
        }
        break;
      case ALTER_TABLE:
        // need to flush any changes before altering the table to ensure all changes before the schema change
        // are in the table when it is altered.
        flush();
        tableId = TableId.of(project, normalizedDatabaseName, normalizedTableName);
        table = bigQuery.getTable(tableId);
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
          .setSchema(Schemas.convert(addSequenceNumber(event.getSchema())))
          .build();
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        if (table == null) {
          bigQuery.create(tableInfo);
        } else {
          bigQuery.update(tableInfo);
        }
        // TODO: alter the staging table as well
        break;
      case RENAME_TABLE:
        // TODO: flush changes, execute a copy job, delete previous table, drop old staging table, remove old entry
        //  in primaryKeyStore, put new entry in primaryKeyStore
        break;
      case TRUNCATE_TABLE:
        // TODO: flush changes then run a DELETE from table WHERE 1=1 query
        break;
    }
    latestOffset = event.getOffset();
    latestSequenceNum = sequencedEvent.getSequenceNumber();
    if (normalizedTableName != null) {
      latestSeenSequence.put(TableId.of(project, normalizedDatabaseName, normalizedTableName),
                             sequencedEvent.getSequenceNumber());
    }
    context.incrementCount(event.getOperation());
    if (event.isSnapshot()) {
      context.setTableSnapshotting(normalizedDatabaseName, normalizedTableName);
    } else {
      context.setTableReplicating(normalizedDatabaseName, normalizedTableName);
    }
  }

  private Schema addSequenceNumber(Schema original) {
    List<Schema.Field> fields = new ArrayList<>(original.getFields().size() + 1);
    fields.add(Schema.Field.of("_sequence_num", Schema.of(Schema.Type.LONG)));
    fields.addAll(original.getFields());
    return Schema.recordOf(original.getRecordName() + ".sequenced", fields);
  }

  private void commitOffset() throws DeltaFailureException {
    try {
      Failsafe.with(commitRetryPolicy).run(() -> {
        if (latestOffset != null) {
          context.commitOffset(latestOffset, latestSequenceNum);
        }
      });
    } catch (Exception e) {
      throw new DeltaFailureException(e.getMessage(), e);
    }
  }

  @Override
  public synchronized void applyDML(Sequenced<DMLEvent> sequencedEvent) throws Exception {
    // this is non-null if an error happened during a time scheduled flush
    if (flushException != null) {
      throw flushException;
    }

    DMLEvent event = sequencedEvent.getEvent();
    String normalizedDatabaseName = normalize(event.getDatabase());
    String normalizedTableName = normalize(event.getTable());
    DMLEvent normalizedDMLEvent = DMLEvent.builder(event)
      .setDatabase(normalizedDatabaseName)
      .setTable(normalizedTableName)
      .build();
    long sequenceNumber = sequencedEvent.getSequenceNumber();
    gcsWriter.write(new Sequenced<>(normalizedDMLEvent, sequenceNumber));

    TableId tableId = TableId.of(project, normalizedDatabaseName, normalizedTableName);
    latestSeenSequence.put(tableId, sequenceNumber);

    if (!latestMergedSequence.containsKey(tableId)) {
      latestMergedSequence.put(tableId, getLatestSequenceNum(tableId));
    }

    latestOffset = event.getOffset();
    latestSequenceNum = sequenceNumber;
    currentBatchSize++;
    context.incrementCount(event.getOperation());
    if (currentBatchSize >= batchMaxRows) {
      flush();
    }

    if (event.isSnapshot()) {
      context.setTableSnapshotting(normalizedDatabaseName, normalizedTableName);
    } else {
      context.setTableReplicating(normalizedDatabaseName, normalizedTableName);
    }
  }

  @VisibleForTesting
  synchronized void flush() throws InterruptedException, IOException, DeltaFailureException {
    long now = System.currentTimeMillis();
    // if there was a flush that recently happened because the max number of events per batch was reached,
    // then skip the time based flush.
    if (now - lastFlushTime < TimeUnit.SECONDS.toMillis(batchMaxSecondsElapsed)) {
      return;
    }
    lastFlushTime = now;

    Collection<TableBlob> tableBlobs;
    // if this throws an IOException, we want to propagate it, since we need the app to reset state to the last
    // commit and replay events. This is because previous events are written directly to an outputstream to GCS
    // and then dropped, so we cannot simply retry the flush here.
    try {
      tableBlobs = gcsWriter.flush();
    } catch (IOException e) {
      flushException = e;
      throw e;
    }

    // TODO: do this in parallel and asynchronously
    for (TableBlob blob : tableBlobs) {
      mergeTableChanges(blob);
    }

    currentBatchSize = 0;
    latestMergedSequence.clear();
    latestMergedSequence.putAll(latestSeenSequence);
    commitOffset();
  }

  private void mergeTableChanges(TableBlob blob) throws DeltaFailureException, InterruptedException {
    String normalizedStagingTableName = normalize(stagingTablePrefix + blob.getTable());
    TableId stagingTableId = TableId.of(project, blob.getDataset(), normalizedStagingTableName);

    runWithRetries(runContext -> loadStagingTable(stagingTableId, blob, runContext.getAttemptCount()),
                   blob,
                   String.format("Failed to load a batch of changes from GCS into staging table for %s.%s",
                                 blob.getDataset(), blob.getTable()),
                   "Exhausted retries while attempting to load changed to the staging table.");

    runWithRetries(runContext -> mergeStagingTable(stagingTableId, blob, runContext.getAttemptCount()),
                   blob,
                   String.format("Failed to merge a batch of changes from the staging table into %s.%s",
                                 blob.getDataset(), blob.getTable()),
                   String.format("Exhausted retries while attempting to merge changes into target table %s.%s. "
                                   + "Check that the service account has the right permissions "
                                   + "and the table was not modified.", blob.getDataset(), blob.getTable()));

    try {
      blob.getBlob().delete();
    } catch (Exception e) {
      // there is no retry for this cleanup error since it will not affect future functionality.
      LOG.warn("Failed to delete temporary GCS object {} in bucket {}. The object will need to be manually deleted.",
               blob.getBlob().getBlobId().getName(), blob.getBlob().getBlobId().getBucket(), e);
    }
  }

  private void loadStagingTable(TableId stagingTableId, TableBlob blob, int attemptNumber) throws InterruptedException {
    LOG.debug("Loading batch {} into staging table for {}.{}", blob.getBatchId(), blob.getDataset(), blob.getTable());
    Table stagingTable = bigQuery.getTable(stagingTableId);
    if (stagingTable == null) {
      TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
        .setLocation(bucket.getLocation())
        .setSchema(Schemas.convert(blob.getStagingSchema()))
        .build();
      TableInfo tableInfo = TableInfo.newBuilder(stagingTableId, tableDefinition)
        .build();
      bigQuery.create(tableInfo);
    }

    // load data from GCS object into staging BQ table
    // batch id is a timestamp generated at the time the first event was seen, so the job id is
    // guaranteed to be different from the previous batch for the table
    JobId jobId = JobId.newBuilder()
      .setLocation(bucket.getLocation())
      .setJob(String.format("%s_stage_%s_%s_%d_%d", context.getApplicationName(), blob.getDataset(),
                            blob.getTable(), blob.getBatchId(), attemptNumber))
      .build();
    BlobId blobId = blob.getBlob().getBlobId();
    String uri = String.format("gs://%s/%s", blobId.getBucket(), blobId.getName());
    LoadJobConfiguration loadJobConf = LoadJobConfiguration.newBuilder(stagingTableId, uri)
      .setFormatOptions(FormatOptions.avro())
      .build();
    JobInfo jobInfo = JobInfo.newBuilder(loadJobConf)
      .setJobId(jobId)
      .build();
    Job loadJob = bigQuery.create(jobInfo);
    loadJob.waitFor();
    LOG.debug("Loaded batch {} into staging table for {}.{}", blob.getBatchId(), blob.getDataset(), blob.getTable());
  }

  private void mergeStagingTable(TableId stagingTableId, TableBlob blob,
                                 int attemptNumber) throws InterruptedException {
    LOG.debug("Merging batch {} for {}.{}", blob.getBatchId(), blob.getDataset(), blob.getTable());
    TableId targetTableId = TableId.of(project, blob.getDataset(), blob.getTable());
    List<String> primaryKey = primaryKeyStore.get(targetTableId);
    if (primaryKey == null) {
      // restore from local
    }
    /*
     * Merge data from staging BQ table into target table.
     *
     * If the source table has two columns -- id and name -- the staging table will look something like:
     *
     * | _batch_id      | _sequence_num | _op    | _before_id | _before_name | id | name
     * | 1234567890     | 2             | INSERT |            |              | 0  | alice
     * | 1234567890     | 3             | UPDATE | 0          | alice        | 1  | alice
     * | 1234567890     | 4             | UPDATE | 1          | alice        | 2  | alice
     * | 1234567890     | 5             | DELETE | 2          | alice        | 2  | alice
     * | 1234567890     | 6             | INSERT |            |              | 0  | Alice
     * | 1234567890     | 7             | INSERT |            |              | 1  | blob
     * | 1234567890     | 8             | UPDATE | 1          | blob         | 1  | Bob
     *
     * If the primary key is the 'id' field, the merge is performed by running the following query:
     *
     * MERGE [target table] as T
     * USING ($DIFF_QUERY) as D
     * ON T.id = D._before_id
     * WHEN MATCHED AND D._op = "DELETE"
     *   DELETE
     * WHEN MATCHED AND D._op IN ("INSERT", "UPDATE")
     *   UPDATE id = D.id, name = D.name
     * WHEN NOT MATCHED AND D._op IN ("INSERT", "UPDATE")
     *   INSERT (id, name) VALUES (id, name)
     *
     * where the $DIFF_QUERY is:
     *
     * SELECT A.* FROM
     *   (SELECT * FROM [staging table] WHERE _batch_id = 1234567890 AND _sequence_num > $LATEST_APPLIED) as A
     *   LEFT OUTER JOIN
     *   (SELECT * FROM [staging table] WHERE _batch_id = 1234567890 AND _sequence_num > $LATEST_APPLIED) as B
     *   ON A.id = B._before_id AND A._sequence_num < B._sequence_num
     *   WHERE B._before_id IS NULL
     *
     * The purpose of the query is to flatten events within the same batch that have the same primary key.
     * For example, with the example data given above, the result of the diff query is:
     *
     * | _batch_id      | _sequence_num | _op    | _before_id | _before_name | id | name
     * | 1234567890     | 5             | DELETE |            |              | 2  | alice
     * | 1234567890     | 6             | INSERT |            |              | 0  | Alice
     * | 1234567890     | 8             | UPDATE |            |              | 1  | Bob
     *
     * The $LATEST_APPLIED part of the query is required for idempotency. If a previous run of the pipeline merged
     * some results into the target table, but died before it could commit its offset, the merge query could end up
     * doing the wrong thing because what goes into a batch is not deterministic due to the time bound on batches.
     */

    /*
     * SELECT A.* FROM
     *   (SELECT * FROM [staging table] WHERE _batch_id = 1234567890 AND _sequence_num > $LATEST_APPLIED) as A
     *   LEFT OUTER JOIN
     *   (SELECT * FROM [staging table] WHERE _batch_id = 1234567890 AND _sequence_num > $LATEST_APPLIED) as B
     *   ON A.id = B._before_id AND A._sequence_num < B._sequence_num
     *   WHERE B._before_id IS NULL
     */
    String diffQuery = "SELECT A.* FROM\n" +
      "(SELECT * FROM " + stagingTableId.getDataset() + "." + stagingTableId.getTable() +
      " WHERE _batch_id = " + blob.getBatchId() +
      " AND _sequence_num > " + latestMergedSequence.get(targetTableId) + ") as A\n" +
      "LEFT OUTER JOIN\n" +
      "(SELECT * FROM " + stagingTableId.getDataset() + "." + stagingTableId.getTable() +
      " WHERE _batch_id = " + blob.getBatchId() +
      " AND _sequence_num > " + latestMergedSequence.get(targetTableId) + ") as B\n" +
      "ON " +
      primaryKey.stream()
        .map(name -> String.format("A.%s = B._before_%s", name, name))
        .collect(Collectors.joining(" AND ")) +
      " AND A._sequence_num < B._sequence_num\n" +
      "WHERE " +
      primaryKey.stream()
        .map(name -> String.format("B._before_%s IS NULL", name))
        .collect(Collectors.joining(" AND "));

    /*
     * MERGE [target table] as T
     * USING ($DIFF_QUERY) as D
     * ON T.id = D._before_id
     * WHEN MATCHED AND D._op = "DELETE" THEN
     *   DELETE
     * WHEN MATCHED AND D._op IN ("INSERT", "UPDATE") THEN
     *   UPDATE id = D.id, name = D.name
     * WHEN NOT MATCHED AND D._op IN ("INSERT", "UPDATE") THEN
     *   INSERT (id, name) VALUES (id, name)
     */
    String query = "MERGE " +
      targetTableId.getDataset() + "." + targetTableId.getTable() + " as T\n" +
      "USING (" + diffQuery + ") as D\n" +
      "ON " +
      primaryKey.stream()
        .map(name -> String.format("T.%s = D._before_%s", name, name))
        .collect(Collectors.joining(" AND ")) + "\n" +
      "WHEN MATCHED AND D._op = \"DELETE\" THEN\n" +
      "  DELETE\n" +
      "WHEN MATCHED AND D._op IN (\"INSERT\", \"UPDATE\") THEN\n" +
      "  UPDATE SET " +
      blob.getTargetSchema().getFields().stream()
        .map(Schema.Field::getName)
        .map(name -> String.format("%s = D.%s", name, name))
        .collect(Collectors.joining(", ")) + "\n" +
      "WHEN NOT MATCHED AND D._op IN (\"INSERT\", \"UPDATE\") THEN\n" +
      "  INSERT (" +
      blob.getTargetSchema().getFields()
        .stream().map(Schema.Field::getName)
        .collect(Collectors.joining(", ")) +
      ") VALUES (" +
      blob.getTargetSchema().getFields()
        .stream().map(Schema.Field::getName)
        .collect(Collectors.joining(", ")) + ")";
    QueryJobConfiguration mergeJobConf = QueryJobConfiguration.newBuilder(query).build();
    // job id will be different even after a retry because batchid is the timestamp when the first
    // event in the batch was seen
    JobId jobId = JobId.newBuilder()
      .setLocation(bucket.getLocation())
      .setJob(String.format("%s_merge_%s_%s_%d_%d", context.getApplicationName(), blob.getDataset(), blob.getTable(),
                            blob.getBatchId(), attemptNumber))
      .build();
    JobInfo jobInfo = JobInfo.newBuilder(mergeJobConf)
      .setJobId(jobId)
      .build();
    Job mergeJob = bigQuery.create(jobInfo);
    mergeJob.waitFor();
    LOG.debug("Merged batch {} into {}.{}", blob.getBatchId(), blob.getDataset(), blob.getTable());
  }

  private void runWithRetries(ContextualRunnable runnable, TableBlob blob, String onFailedAttemptMessage,
                              String retriesExhaustedMessage) throws DeltaFailureException, InterruptedException {
    RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
      .withMaxDuration(Duration.of(context.getMaxRetrySeconds(), ChronoUnit.SECONDS))
      // maxDuration must be greater than the delay, otherwise Failsafe complains
      .withBackoff(Math.min(61, context.getMaxRetrySeconds()) - 1, 120, ChronoUnit.SECONDS)
      .withMaxAttempts(Integer.MAX_VALUE)
      .onFailedAttempt(failureContext -> {
        Throwable t = failureContext.getLastFailure();
        LOG.error(onFailedAttemptMessage, t);
        // its ok to set table state every retry, because this is a no-op if there is no change to the state.
        try {
          context.setTableError(blob.getDataset(), blob.getTable(), new ReplicationError(t));
        } catch (IOException e) {
          // setting table state is not a fatal error, log a warning and continue on
          LOG.warn("Unable to set error state for table {}.{}. Replication state for the table may be incorrect.",
                   blob.getDataset(), blob.getTable());
        }
      });

    try {
      Failsafe.with(retryPolicy).run(runnable);
    } catch (TimeoutExceededException e) {
      // if the retry timeout was reached, throw a DeltaFailureException to fail the pipeline immediately
      DeltaFailureException exc = new DeltaFailureException(retriesExhaustedMessage, e);
      flushException = exc;
      throw exc;
    } catch (FailsafeException e) {
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      throw e;
    }
  }

  private long getLatestSequenceNum(TableId tableId) throws InterruptedException, DeltaFailureException {
    RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
      .withMaxDuration(Duration.of(context.getMaxRetrySeconds(), ChronoUnit.SECONDS))
      // maxDuration must be greater than the delay, otherwise Failsafe complains
      .withBackoff(Math.min(61, context.getMaxRetrySeconds()) - 1, 120, ChronoUnit.SECONDS)
      .withMaxAttempts(Integer.MAX_VALUE)
      .onFailedAttempt(failureContext -> {
        Throwable t = failureContext.getLastFailure();
        LOG.error("Failed to read maximum sequence number from {}.{}.", tableId.getDataset(), tableId.getTable(), t);
      });

    try {
      return Failsafe.with(retryPolicy).get(() -> {
        if (bigQuery.getTable(tableId) == null) {
          return 0L;
        }

        String query = String.format("SELECT MAX(_sequence_num) FROM %s.%s", tableId.getDataset(), tableId.getTable());
        QueryJobConfiguration jobConfig = QueryJobConfiguration.of(query);
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(jobConfig).setJobId(jobId).build());
        queryJob.waitFor();
        TableResult result = queryJob.getQueryResults();
        Iterator<FieldValueList> resultIter = result.iterateAll().iterator();
        if (!resultIter.hasNext()) {
          return 0L;
        }
        // query is SELECT MAX(_sequence_num) FROM ...
        // so there is at most one row and one column in the output.
        FieldValue val = resultIter.next().get(0);
        if (val.getValue() == null) {
          return 0L;
        }

        long answer = val.getLongValue();
        LOG.debug("Loaded {} as the latest merged sequence number for {}.{}",
                  answer, tableId.getDataset(), tableId.getTable());
        return answer;
      });
    } catch (TimeoutExceededException e) {
      // if the retry timeout was reached, throw a DeltaFailureException to fail the pipeline immediately
      throw new DeltaFailureException(e.getMessage(), e);
    } catch (FailsafeException e) {
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      throw e;
    }
  }

  public static String normalize(String name) {
    // replace invalid chars with underscores if there are any
    if (!name.matches(VALID_NAME_REGEX)) {
      name = name.replaceAll(INVALID_NAME_REGEX, "_");
    }

    // truncate the name if it exceeds the max length
    if (name.length() > MAX_LENGTH) {
      name = name.substring(0, MAX_LENGTH);
    }
    return name;
  }
}
