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
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.EncryptionConfiguration;
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
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
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
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

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
  private static final Gson GSON = new Gson();

  private final DeltaTargetContext context;
  private final BigQuery bigQuery;
  private final int loadIntervalSeconds;
  private final String stagingTablePrefix;
  private final MultiGCSWriter gcsWriter;
  private final Bucket bucket;
  private final String project;
  private final EncryptionConfiguration encryptionConfig;
  private final RetryPolicy<Object> commitRetryPolicy;
  private final Map<TableId, Long> latestSeenSequence;
  private final Map<TableId, Long> latestMergedSequence;
  private final Map<TableId, List<String>> primaryKeyStore;
  private final boolean requireManualDrops;
  private final long baseRetryDelay;
  private final int maxClusteringColumns;
  private final boolean sourceRowIdSupported;
  private ScheduledExecutorService scheduledExecutorService;
  private ScheduledFuture<?> scheduledFlush;
  private ExecutorService executorService;
  private Offset latestOffset;
  private long latestSequenceNum;
  private Exception flushException;
  // have to keep all the records in memory in case there is a failure writing to GCS
  // cannot write to a temporary file on local disk either in case there is a failure writing to disk
  // Without keeping the entire batch in memory, there would be no way to recover the records that failed to write

  BigQueryEventConsumer(DeltaTargetContext context, Storage storage, BigQuery bigQuery, Bucket bucket,
                        String project, int loadIntervalSeconds, String stagingTablePrefix, boolean requireManualDrops,
                        @Nullable EncryptionConfiguration encryptionConfig, @Nullable Long baseRetryDelay) {
    this.context = context;
    this.bigQuery = bigQuery;
    this.loadIntervalSeconds = loadIntervalSeconds;
    this.stagingTablePrefix = stagingTablePrefix;
    this.bucket = bucket;
    this.project = project;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.latestSequenceNum = 0L;
    this.encryptionConfig = encryptionConfig;
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
    this.requireManualDrops = requireManualDrops;
    this.executorService = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("bq-daemon-%d"));
    this.gcsWriter = new MultiGCSWriter(storage, bucket.getName(),
                                        String.format("cdap/delta/%s/", context.getApplicationName()),
                                        context, executorService);
    this.baseRetryDelay = baseRetryDelay == null ? 10L : baseRetryDelay;
    String maxClusteringColumnsStr = context.getRuntimeArguments().get("gcp.bigquery.max.clustering.columns");
    // current max clustering columns is set as 4 in big query side, use that as default max value
    // https://cloud.google.com/bigquery/docs/creating-clustered-tables#limitations
    this.maxClusteringColumns = maxClusteringColumnsStr == null ? 4 : Integer.parseInt(maxClusteringColumnsStr);
    this.sourceRowIdSupported =
      context.getSourceProperties() == null ? false : context.getSourceProperties().isRowIdSupported();
  }

  @Override
  public void start() {
    scheduledFlush = scheduledExecutorService.scheduleAtFixedRate(() -> {
      try {
        flush();
      } catch (InterruptedException e) {
        // just return and let things end
      } catch (Exception e) {
        flushException = e;
      }
    }, loadIntervalSeconds, loadIntervalSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    if (scheduledFlush != null) {
      scheduledFlush.cancel(true);
    }
    scheduledExecutorService.shutdownNow();
    executorService.shutdownNow();
    try {
      scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
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
    DDLOperation ddlOperation = event.getOperation();
    String normalizedDatabaseName = normalize(event.getOperation().getDatabaseName());
    String normalizedTableName = normalize(ddlOperation.getTableName());
    String normalizedStagingTableName = normalizedTableName == null ? null :
      normalize(stagingTablePrefix + normalizedTableName);

    try {
      Failsafe.with(createBaseRetryPolicy(baseRetryDelay)).run(() -> {
        handleDDL(event, normalizedDatabaseName, normalizedTableName, normalizedStagingTableName);
      });
    } catch (TimeoutExceededException e) {
      throw new DeltaFailureException(
        String.format("Exhausted retries trying to apply '%s' DDL event", event.getOperation()), e);
    } catch (FailsafeException e) {
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      if (e.getCause() instanceof DeltaFailureException) {
        throw (DeltaFailureException) e.getCause();
      }
      throw e;
    }


    latestOffset = event.getOffset();
    latestSequenceNum = sequencedEvent.getSequenceNumber();
    LOG.info("Setting latest offset and seq num for DDL event : {} with seq num : {}", event.getOperation().getType()
      , latestSequenceNum);
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

  private void handleDDL(DDLEvent event, String normalizedDatabaseName, String normalizedTableName,
                         String normalizedStagingTableName)
    throws IOException, DeltaFailureException, InterruptedException {

    switch (event.getOperation().getType()) {
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
          if (requireManualDrops) {
            String message = String.format("Encountered an event to drop dataset '%s' in project '%s', " +
                                             "but the target is configured to require manual drops. " +
                                             "Please manually drop the dataset to make progress.",
                                           normalizedDatabaseName, project);
            LOG.error(message);
            throw new RuntimeException(message);
          }
          bigQuery.delete(datasetId);
        }
        break;
      case CREATE_TABLE:
        TableId tableId = TableId.of(project, normalizedDatabaseName, normalizedTableName);
        Table table = bigQuery.getTable(tableId);
        List<String> primaryKeys = event.getPrimaryKey();
        updatePrimaryKeys(tableId, primaryKeys);
        // TODO: check schema of table if it exists already
        if (table == null) {
          Clustering clustering = maxClusteringColumns <= 0 ? null :
            Clustering.newBuilder()
              .setFields(primaryKeys.subList(0, Math.min(maxClusteringColumns, primaryKeys.size())))
              .build();
          TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(Schemas.convert(addSupplementaryColumnsToTargetSchema(event.getSchema())))
            .setClustering(clustering)
            .build();

          TableInfo.Builder builder = TableInfo.newBuilder(tableId, tableDefinition);
          if (encryptionConfig != null) {
            builder.setEncryptionConfiguration(encryptionConfig);
          }
          TableInfo tableInfo = builder.build();
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
          if (requireManualDrops) {
            String message = String.format("Encountered an event to drop table '%s' in dataset '%s' in project '%s', " +
                                             "but the target is configured to require manual drops. " +
                                             "Please manually drop the table to make progress.",
                                           normalizedTableName, normalizedDatabaseName, project);
            LOG.error(message);
            throw new RuntimeException(message);
          }
          bigQuery.delete(tableId);
        }
        TableId stagingTableId = TableId.of(project, normalizedDatabaseName, normalizedStagingTableName);
        Table stagingTable = bigQuery.getTable(stagingTableId);
        if (stagingTable != null) {
          bigQuery.delete(stagingTableId);
        }
        break;
      case ALTER_TABLE:
        // need to flush any changes before altering the table to ensure all changes before the schema change
        // are in the table when it is altered.
        flush();
        // after a flush, the staging table will be gone, so no need to alter it.
        tableId = TableId.of(project, normalizedDatabaseName, normalizedTableName);
        table = bigQuery.getTable(tableId);
        primaryKeys = event.getPrimaryKey();
        Clustering clustering = maxClusteringColumns <= 0 ? null :
          Clustering.newBuilder()
            .setFields(primaryKeys.subList(0, Math.min(maxClusteringColumns, primaryKeys.size())))
            .build();
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
          .setSchema(Schemas.convert(addSupplementaryColumnsToTargetSchema(event.getSchema())))
          .setClustering(clustering)
          .build();
        TableInfo.Builder builder = TableInfo.newBuilder(tableId, tableDefinition);
        if (encryptionConfig != null) {
          builder.setEncryptionConfiguration(encryptionConfig);
        }
        TableInfo tableInfo = builder.build();
        if (table == null) {
          bigQuery.create(tableInfo);
        } else {
          bigQuery.update(tableInfo);
        }

        updatePrimaryKeys(tableId, primaryKeys);
        break;
      case RENAME_TABLE:
        // TODO: flush changes, execute a copy job, delete previous table, drop old staging table, remove old entry
        //  in primaryKeyStore, put new entry in primaryKeyStore
        LOG.warn("Rename DDL events are not supported. Ignoring rename event in database {} from table {} to table {}.",
          event.getOperation().getDatabaseName(), event.getOperation().getPrevTableName(),
          event.getOperation().getTableName());
        break;
      case TRUNCATE_TABLE:
        flush();
        tableId = TableId.of(project, normalizedDatabaseName, normalizedTableName);
        table = bigQuery.getTable(tableId);
        if (table != null) {
          tableDefinition = table.getDefinition();
          bigQuery.delete(tableId);
        } else {
          primaryKeys = event.getPrimaryKey();
          clustering = maxClusteringColumns <= 0 ? null :
            Clustering.newBuilder()
              .setFields(primaryKeys.subList(0, Math.min(maxClusteringColumns, primaryKeys.size())))
              .build();
          tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(Schemas.convert(addSupplementaryColumnsToTargetSchema(event.getSchema())))
            .setClustering(clustering)
            .build();
        }

        builder = TableInfo.newBuilder(tableId, tableDefinition);
        if (encryptionConfig != null) {
          builder.setEncryptionConfiguration(encryptionConfig);
        }
        tableInfo = builder.build();
        bigQuery.create(tableInfo);
        break;
    }
  }

  private void updatePrimaryKeys(TableId tableId, List<String> primaryKeys) throws DeltaFailureException, IOException {
    if (primaryKeys.isEmpty()) {
      throw new DeltaFailureException(
        String.format("Table '%s' in database '%s' has no primary key. Tables without a primary key are" +
                        " not supported.", tableId.getTable(), tableId.getDataset()));
    }
    List<String> existingKey = primaryKeyStore.get(tableId);
    if (primaryKeys.equals(existingKey)) {
      return;
    }
    primaryKeyStore.put(tableId, primaryKeys);
    context.putState(String.format("bigquery-%s-%s", tableId.getDataset(), tableId.getTable()),
                     Bytes.toBytes(GSON.toJson(new BigQueryTableState(primaryKeys))));
  }

  private List<String> getPrimaryKeys(TableId targetTableId) throws IOException, DeltaFailureException {
    List<String> primaryKeys = primaryKeyStore.get(targetTableId);
    if (primaryKeys == null) {
      byte[] stateBytes = context.getState(
        String.format("bigquery-%s-%s", targetTableId.getDataset(), targetTableId.getTable()));
      if (stateBytes == null) {
        throw new DeltaFailureException(
          String.format("Primary key information for table '%s' in dataset '%s' could not be found. This can only " +
                          "happen if state was corrupted. Please create a new replicator and start again.",
                        targetTableId.getTable(), targetTableId.getDataset()));
      }
      BigQueryTableState targetTableState = GSON.fromJson(new String(stateBytes), BigQueryTableState.class);
      primaryKeys = targetTableState.getPrimaryKeys();
    }
    return primaryKeys;
  }

  private Schema addSupplementaryColumnsToTargetSchema(Schema original) {
    List<Schema.Field> fields = new ArrayList<>(original.getFields().size() + 3);
    fields.add(Schema.Field.of(Constants.SEQUENCE_NUM, Schema.of(Schema.Type.LONG)));
    fields.add(Schema.Field.of(Constants.IS_DELETED, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
    fields.add(Schema.Field.of(Constants.ROW_ID, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    fields.addAll(original.getFields());
    return Schema.recordOf(original.getRecordName() + ".sequenced", fields);
  }

  private void commitOffset() throws DeltaFailureException {
    try {
      Failsafe.with(commitRetryPolicy).run(() -> {
        if (latestOffset != null) {
          LOG.info("Committing offset : {} and seq num: {}" , latestOffset.get(), latestSequenceNum);
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
    String normalizedDatabaseName = normalize(event.getOperation().getDatabaseName());
    String normalizedTableName = normalize(event.getOperation().getTableName());
    DMLEvent normalizedDMLEvent = DMLEvent.builder(event)
      .setDatabaseName(normalizedDatabaseName)
      .setTableName(normalizedTableName)
      .build();
    long sequenceNumber = sequencedEvent.getSequenceNumber();
    gcsWriter.write(new Sequenced<>(normalizedDMLEvent, sequenceNumber));

    TableId tableId = TableId.of(project, normalizedDatabaseName, normalizedTableName);

    Long latestMergedSequencedNum = latestMergedSequence.get(tableId);
    if (latestMergedSequencedNum == null) {
      // first event of the table
      latestMergedSequencedNum  = getLatestSequenceNum(tableId);
      latestMergedSequence.put(tableId, latestMergedSequencedNum);
      // latestSeenSequence will replace the latestMergedSequence at the end of flush()
      // set this default value to avoid dup query of max merged sequence num in next `flush()`
      latestSeenSequence.put(tableId, latestMergedSequencedNum);
    }

    // it's possible that some previous events were merged to target table but offset were not committed
    // because offset is committed when the whole batch of all the tables were merged.
    // so it's possible we see an event that was already merged to target table
    if (sequenceNumber > latestMergedSequencedNum) {
      latestSeenSequence.put(tableId, sequenceNumber);
    }

    latestOffset = event.getOffset();
    latestSequenceNum = sequenceNumber;
    LOG.info("Setting latest offset and seq num for DML event : {} with seq num : {}", event.getRow().get("id"),
      latestSequenceNum);
    context.incrementCount(event.getOperation());

    if (event.isSnapshot()) {
      context.setTableSnapshotting(normalizedDatabaseName, normalizedTableName);
    } else {
      context.setTableReplicating(normalizedDatabaseName, normalizedTableName);
    }
  }

  @VisibleForTesting
  synchronized void flush() throws InterruptedException, IOException, DeltaFailureException {
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

    List<Future<?>> mergeFutures = new ArrayList<>(tableBlobs.size());
    for (TableBlob blob : tableBlobs) {
      // submit a callable instead of a runnable so that it can throw checked exceptions
      mergeFutures.add(executorService.submit((Callable<Void>) () -> {
        mergeTableChanges(blob);
        return null;
      }));
    }

    DeltaFailureException exception = null;
    for (Future mergeFuture : mergeFutures) {
      try {
        getMergeFuture(mergeFuture);
      } catch (InterruptedException e) {
        throw e;
      } catch (DeltaFailureException e) {
        if (exception != null) {
          exception.addSuppressed(e);
        } else {
          exception = e;
        }
      }
    }
    if (exception != null) {
      throw exception;
    }

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
    // clean up staging table after merging is done, there is no retry for this clean up since it will not affect
    // future functionality
    bigQuery.delete(stagingTableId);
  }

  private void loadStagingTable(TableId stagingTableId, TableBlob blob, int attemptNumber)
    throws InterruptedException, IOException, DeltaFailureException {
    LOG.debug("Loading batch {} of {} events into staging table for {}.{}",
              blob.getBatchId(), blob.getNumEvents(), blob.getDataset(), blob.getTable());
    Table stagingTable = bigQuery.getTable(stagingTableId);
    if (stagingTable == null) {
      List<String> primaryKeys = getPrimaryKeys(TableId.of(project, blob.getDataset(), blob.getTable()));
      Clustering clustering = maxClusteringColumns <= 0 ? null : Clustering.newBuilder()
        .setFields(primaryKeys.subList(0, Math.min(maxClusteringColumns, primaryKeys.size())))
        .build();
      TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
        .setLocation(bucket.getLocation())
        .setSchema(Schemas.convert(blob.getStagingSchema()))
        .setClustering(clustering)
        .build();
      TableInfo.Builder builder = TableInfo.newBuilder(stagingTableId, tableDefinition);
      if (encryptionConfig != null) {
        builder.setEncryptionConfiguration(encryptionConfig);
      }
      TableInfo tableInfo = builder.build();
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
    LoadJobConfiguration.Builder jobConfigBuilder = LoadJobConfiguration.newBuilder(stagingTableId, uri)
      .setFormatOptions(FormatOptions.avro());
    if (encryptionConfig != null) {
      jobConfigBuilder.setDestinationEncryptionConfiguration(encryptionConfig);
    }
    LoadJobConfiguration loadJobConf = jobConfigBuilder.build();
    JobInfo jobInfo = JobInfo.newBuilder(loadJobConf)
      .setJobId(jobId)
      .build();
    Job loadJob = bigQuery.create(jobInfo);
    loadJob.waitFor();
    LOG.debug("Loaded batch {} into staging table for {}.{}", blob.getBatchId(), blob.getDataset(), blob.getTable());
  }

  private void mergeStagingTable(TableId stagingTableId, TableBlob blob,
                                 int attemptNumber) throws InterruptedException, IOException, DeltaFailureException {
    LOG.debug("Merging batch {} for {}.{}", blob.getBatchId(), blob.getDataset(), blob.getTable());
    TableId targetTableId = TableId.of(project, blob.getDataset(), blob.getTable());
    List<String> primaryKeys = getPrimaryKeys(targetTableId);
    /*
     * Merge data from staging BQ table into target table.
     *
     * Two independent cases to be considered while performing merge operation:
     *
     * Case 1: Source generates events without row id.
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
     *
     *
     * Case 2: Source generates events with row id.
     *
     * If the source table has two columns -- id and name -- the staging table will look something like:
     *
     * | _batch_id      | _sequence_num     | _op    | _row_id | id | name
     * | 1234567890     |       20          | INSERT |  ABCD   | 0  | alice
     * | 1234567890     |       40          | UPDATE |  ABCD   | 2  | alice
     * | 1234567890     |       50          | DELETE |  ABCD   | 2  | alice
     * | 1234567890     |       60          | INSERT |  ABCD   | 0  | alice
     * | 1234567890     |       70          | INSERT |  ABCE   | 1  | bob
     * | 1234567890     |       30          | UPDATE |  ABCD   | 1  | alice
     * | 1234567890     |       80          | UPDATE |  ABCE   | 1  | Bob
     * | 1234567890     |       20          | INSERT |  ABCD   | 0  | alice
     *
     * Merge in this case is performed by running following query:
     *
     * MERGE [target table] as T
     *  USING ($DIFF_QUERY) as D
     * ON T.row_id = D.row_id
     * WHEN MATCHED AND D.op = “DELETE”
     *  UPDATE _is_deleted = true
     * WHEN MATCHED AND D.op IN (“INSERT”, “UPDATE”)
     *              AND D._sequence_num > T._sequence_num
     *    UPDATE _sequence_num = D._sequence_num, id = D.id, name = D.name
     * WHEN NOT MATCHED
     *    INSERT (_sequence_num, _row_id, _is_deleted, id, name) VALUES
     *           (D._sequence_num, D._row_id, false, D.id, D.name)
     *
     *
     * where the $DIFF_QUERY is:
     *
     * SELECT A.* FROM
     *      (SELECT * FROM [staging table]
     *          WHERE batch_id = 12345 AND _sequence_num > $LATEST_APPLIED) as A
     *      LEFT OUTER JOIN
     *      (SELECT * FROM [staging table]
     *          WHERE _batch_id = 12345 AND _sequence_num > $LATEST_APPLIED) as B
     *      ON A._row_id = B._row_id AND A._sequence_num < B._sequence_num
     * WHERE B._row_id IS NULL
     *
     * Similar to the case of events without row id, the above $DIFF_QUERY flattens events within same batch
     * that have the same _row_id and finds out the latest event.
     *
     * The result of $DIFF_QUERY for the example above would be:
     *
     * | _batch_id      | _sequence_num     | _op    | _row_id | id | name
     * | 1234567890     |       60          | INSERT |  ABCD   | 0  | alice
     * | 1234567890     |       80          | UPDATE |  ABCE   | 1  | Bob
     */

    String diffQuery = createDiffQuery(stagingTableId, primaryKeys, blob.getBatchId(),
                                       latestMergedSequence.get(targetTableId), sourceRowIdSupported);

    String mergeQuery =
      createMergeQuery(targetTableId, primaryKeys, blob.getTargetSchema(), diffQuery, sourceRowIdSupported);

    QueryJobConfiguration.Builder jobConfigBuilder = QueryJobConfiguration.newBuilder(mergeQuery);
    if (encryptionConfig != null) {
      jobConfigBuilder.setDestinationEncryptionConfiguration(encryptionConfig);
    }
    QueryJobConfiguration mergeJobConf = jobConfigBuilder.build();
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

  static String createDiffQuery(TableId stagingTable, List<String> primaryKeys, long batchId,
                                Long latestSequenceNumInTargetTable, boolean sourceRowIdSupported) {
    String joinCondition;
    String whereClause;
    if (sourceRowIdSupported) {
      /*
       * Query will be of the form:
       *
       * SELECT A.* FROM
       *   (SELECT * FROM [staging table] WHERE _batch_id = 1234567890 AND _sequence_num > $LATEST_APPLIED) as A
       *   LEFT OUTER JOIN
       *   (SELECT * FROM [staging table] WHERE _batch_id = 1234567890 AND _sequence_num > $LATEST_APPLIED) as B
       *   ON A._row_id = B._row_id AND A._sequence_num < B._sequence_num
       *   WHERE B._row_id IS NULL
       *
       * Here we only construct the join condition and where clause as that is the only difference
       * based on where source events support row id.
       */

      joinCondition =  "A._row_id = B._row_id ";
      whereClause = " B._row_id IS NULL ";
    } else {
      /*
       * Query will be of the form:
       *
       * SELECT A.* FROM
       *   (SELECT * FROM [staging table] WHERE _batch_id = 1234567890 AND _sequence_num > $LATEST_APPLIED) as A
       *   LEFT OUTER JOIN
       *   (SELECT * FROM [staging table] WHERE _batch_id = 1234567890 AND _sequence_num > $LATEST_APPLIED) as B
       *   ON A.id = B._before_id AND A._sequence_num < B._sequence_num
       *   WHERE B._before_id IS NULL
       *
       * Here we only construct the join condition and where clause as that is the only difference
       * based on ordering of events.
       */
      joinCondition = primaryKeys.stream()
        .map(name -> String.format("A.%s = B._before_%s", name, name))
        .collect(Collectors.joining(" AND "));

      whereClause = primaryKeys.stream()
        .map(name -> String.format("B._before_%s IS NULL", name))
        .collect(Collectors.joining(" AND "));
    }

    return "SELECT A.* FROM\n" +
      "(SELECT * FROM " + stagingTable.getDataset() + "." + stagingTable.getTable() +
      " WHERE _batch_id = " + batchId +
      " AND _sequence_num > " + latestSequenceNumInTargetTable + ") as A\n" +
      "LEFT OUTER JOIN\n" +
      "(SELECT * FROM " + stagingTable.getDataset() + "." + stagingTable.getTable() +
      " WHERE _batch_id = " + batchId +
      " AND _sequence_num > " + latestSequenceNumInTargetTable + ") as B\n" +
      "ON " + joinCondition + " AND A._sequence_num < B._sequence_num\n" +
      "WHERE " + whereClause;
  }

  static String createMergeQuery(TableId targetTableId, List<String> primaryKeys, Schema targetSchema,
                                 String diffQuery, boolean sourceRowIdSupported) {
    String mergeCondition;
    String deleteOperation;

    if (sourceRowIdSupported) {
      /*
       * Merge query will be of the following form:
       *
       * MERGE [target table] as T
       *  USING ($DIFF_QUERY) as D
       * ON T._row_id = D._row_id
       * WHEN MATCHED AND D.op = “DELETE”
       *    UPDATE _is_deleted = true
       * WHEN MATCHED AND D.op IN (“UPDATE”)
       *              AND D._sequence_num > T._sequence_num
       *    UPDATE _sequence_num = D._sequence_num, id = D.id, name = D.name
       * WHEN NOT MATCHED
       *    INSERT (_sequence_num, _row_id, _is_deleted, id, name) VALUES
       *           (D._sequence_num, D._row_id, false, D.id, D.name)
       *
       * Following are the differences compared to merge query for events with row id.
       * 1. join condition used for the merge query includes _row_id.
       * 2. when match happens for DELETE operation, update _is_deleted in the target table to true
       * 3. when no match insert the new row
       */
      mergeCondition = " T._row_id = D._row_id ";
      deleteOperation = "  UPDATE SET _is_deleted = true ";
    } else {
      /*
       * Merge query will be of the following form:
       *
       * MERGE [target table] as T
       * USING ($DIFF_QUERY) as D
       * ON T.id = D._before_id
       * WHEN MATCHED AND D._op = "DELETE" THEN
       *   DELETE
       * WHEN MATCHED AND D._op IN ("INSERT", "UPDATE") THEN
       *   UPDATE id = D.id, name = D.name
       * WHEN NOT MATCHED AND D._op IN ("INSERT", "UPDATE") THEN
       *   INSERT (id, name) VALUES (id, name)
       *
       * Following are the differences compared to merge query for events with row id.
       * 1. join condition used for the merge query includes primary keys.
       * 2. when match happens for DELETE operation, issue DELETE command
       */
      mergeCondition = primaryKeys.stream()
        .map(name -> String.format("T.%s = D._before_%s", name, name))
        .collect(Collectors.joining(" AND "));

      deleteOperation = "  DELETE";
    }

    // target table schema always contains nullable fields such as _is_deleted, _row_id.
    // however for ORDERED source, these fields are not in the staging table, so filter out these fields before
    // we perform INSERT/UPDATE queries based on the values from the staging table.
    final Predicate<Schema.Field> predicate = field -> {
      // filter out _is_deleted field for operations INSERT and UPDATE as it will be set when
      // the operation is DELETE.
      if (field.getName().equals(Constants.IS_DELETED)) {
        return false;
      }
      if (sourceRowIdSupported) {
        return true;
      }
      // filter out the _row_id field for ORDERED source, as this field will not be present in the staging table.
      return !field.getName().equals(Constants.ROW_ID);
    };

    return "MERGE " +
      targetTableId.getDataset() + "." + targetTableId.getTable() + " as T\n" +
      "USING (" + diffQuery + ") as D\n" +
      "ON " + mergeCondition + "\n" +
      "WHEN MATCHED AND D._op = \"DELETE\" THEN\n" +
      deleteOperation + "\n" +
      // In a case when a replicator is paused for too long and crashed when resumed
      // user will create a new replicator against the same target
      // in this case the target already has some data
      // so the new repliator's snapshot will generate insert events that match some existing data in the
      // targe. That's why in the match case, we still need the insert opertion.
      "WHEN MATCHED AND D._op IN (\"INSERT\", \"UPDATE\") AND D._sequence_num > T._sequence_num THEN\n" +
      "  UPDATE SET " +
      targetSchema.getFields().stream()
        .filter(predicate)
        .map(Schema.Field::getName)
        .map(name -> String.format("%s = D.%s", name, name))
        .collect(Collectors.joining(", ")) + "\n" +
      "WHEN NOT MATCHED AND D._op IN (\"INSERT\", \"UPDATE\") THEN\n" +
      "  INSERT (" +
      targetSchema.getFields().stream()
        .filter(predicate)
        .map(Schema.Field::getName)
        .collect(Collectors.joining(", ")) +
      ") VALUES (" +
      targetSchema.getFields().stream()
        .filter(predicate)
        .map(Schema.Field::getName)
        .collect(Collectors.joining(", ")) + ")";
  }

  private void runWithRetries(ContextualRunnable runnable, TableBlob blob, String onFailedAttemptMessage,
                              String retriesExhaustedMessage) throws DeltaFailureException, InterruptedException {
    long baseDelay = Math.min(91, context.getMaxRetrySeconds()) - 1;
    RetryPolicy<Object> retryPolicy = createBaseRetryPolicy(baseDelay)
      .onFailedAttempt(failureContext -> {
        Throwable t = failureContext.getLastFailure();
        LOG.error(onFailedAttemptMessage, t);
        // its ok to set table state every retry, because this is a no-op if there is no change to the state.
        try {
          context.setTableError(blob.getDataset(), blob.getSourceDbSchemaName(), blob.getTable(),
                                new ReplicationError(t));
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
      if (e.getCause() instanceof DeltaFailureException) {
        throw (DeltaFailureException) e.getCause();
      }
      throw e;
    }
  }

  private long getLatestSequenceNum(TableId tableId) throws InterruptedException, DeltaFailureException {
    RetryPolicy<Object> retryPolicy = createBaseRetryPolicy(baseRetryDelay)
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
        QueryJobConfiguration.Builder jobConfigBuilder = QueryJobConfiguration.newBuilder(query);
        if (encryptionConfig != null) {
          jobConfigBuilder.setDestinationEncryptionConfiguration(encryptionConfig);
        }
        QueryJobConfiguration jobConfig = jobConfigBuilder.build();
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
    if (name == null) {
      // avoid potential NPE
      return null;
    }

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

  /**
   * Utility method that unwraps ExecutionExceptions and propagates their cause as-is when possible.
   * Expects to be given a Future for a call to mergeTableChanges.
   */
  private static <T> T getMergeFuture(Future<T> mergeFuture) throws InterruptedException, DeltaFailureException {
    try {
      return mergeFuture.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof DeltaFailureException) {
        throw (DeltaFailureException) cause;
      }
      if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      }
      // should not happen unless mergeTables is changed without changing this.
      throw new RuntimeException(cause.getMessage(), cause);
    }
  }

  private <T> RetryPolicy<T> createBaseRetryPolicy(long baseDelay) {
    RetryPolicy<T> retryPolicy = new RetryPolicy<>();
    if (context.getMaxRetrySeconds() < 1) {
      return retryPolicy.withMaxAttempts(1);
    }

    long maxDelay = Math.max(baseDelay + 1, loadIntervalSeconds);
    return retryPolicy.withMaxAttempts(Integer.MAX_VALUE)
      .withBackoff(baseDelay, maxDelay, ChronoUnit.SECONDS);
  }
}
