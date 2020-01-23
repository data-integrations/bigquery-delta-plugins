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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.EventConsumer;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.Sequenced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * Step 1 - Write a batch of changes to GCS exactly once
 *
 * Each batch of changes is written to GCS as an object with path:
 *
 *   [staging bucket]/cdap/cdc/[app name]/[table id]/[batch id]
 *
 * The size of the batch is determined through configuration.
 * There is a maximum number of rows to include in each batch and a maximum amount of time to wait in between batches.
 * If the incoming events contain a transaction id, the maximums will be ignored in favor of including all events from
 * the same transaction in the same batch. The assumption is that events for the same transaction will be applied
 * in order, with no events from other transactions interleaved in between.
 * Each object is written in avro format and contains the columns in the destination table plus two additional columns:
 *   _op: CREATE | UPDATE | DELETE
 *   _batch_id: the batch id
 *
 * Changes in the batch do not span across a DDL event, so they are guaranteed to conform to the same schema.
 * Once the object is successfully written, the offset of the latest DMLEvent within the batch is persisted.
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
 *
 * Step 2 - Load data from GCS into staging BigQuery table exactly once
 *
 * This step happens after the offset from Step 1 is successfully persisted. This will load the object
 * into a staging table in BigQuery. The staging table has the same schema as the rows in the GCS object.
 * It is clustered on _batch_id.
 * The job id for the load is of the form [app name]_stage_[batch id]_[retry num].
 * Failure scenarios are:
 *
 *   1. The load job fails for some reason. For example, permissions were revoked, quota was hit, temporary outage, etc.
 *      The load will be repeatedly retried until is succeeds. It may need manual intervention to succeed.
 *   2. The program dies. When the program starts up again, batches are found in the staging directory on GCS.
 *      The batches are checked in order of their write time. For each batch, the consumer will check if a
 *      corresponding BigQuery job exists. If not, it will create a load job and continue as normal. If the job
 *      exists and it is still running, it will wait for it to finish. If the job exists and it failed, the consumer
 *      will look for retry jobs. If all retry jobs have failed, it will continue retrying.
 *
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
 * The job id is of the form [app name]_merge_staging_[batch id]_[retry_num].
 * This query ensures that it does not matter if there are duplicate events in the batch objects on GCS.
 * Duplicate inserts and deletes will not match and be ignored.
 * Duplicate updates will update the target row to be the same that it already is.
 * Once the job succeeds, the corresponding GCS object is deleted. Failure scenarios are:
 *
 *   1. The merge query fails for some reason. The consumer will retry until it succeeds.
 *      It may need manual intervention to succeed.
 *   2. The program dies. When the program starts up again, batch are found in the staging directory on GCS.
 *      BigQuery jobs are listed to determine which state the program was in before it died.
 *        a. No merge job for the batch is found. The consumer creates the merge job and continues normally.
 *        b. The merge job for the batch is found and successful. The consumer deletes the GCS object and continues
 *           normally.
 *        c. A running merge job for the batch is found. The consumer waits for it to finish and continues normally.
 *        d. A merge job for the batch is found that has failed. The consumer retries the job until it succeeds.
 *   3. The GCS delete fails. The consumer keeps retrying until it succeeds.
 *
 */
public class BigQueryEventConsumer implements EventConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryEventConsumer.class);
  private final DeltaTargetContext context;
  private final BigQuery bigQuery;
  private final int batchMaxRows;
  private final int batchMaxSecondsElapsed;
  private final MultiGCSWriter gcsWriter;
  private final Bucket bucket;
  private final String project;
  private ScheduledExecutorService executorService;
  private ScheduledFuture<?> scheduledFlush;
  private Offset latestOffset;
  private int currentBatchSize;
  // have to keep all the records in memory in case there is a failure writing to GCS
  // cannot write to a temporary file on local disk either in case there is a failure writing to disk
  // Without keeping the entire batch in memory, there would be no way to recover the records that failed to write

  public BigQueryEventConsumer(DeltaTargetContext context, Storage storage, BigQuery bigQuery, Bucket bucket,
                               String project, int batchMaxRows, int batchMaxSecondsElapsed) {
    this.context = context;
    this.bigQuery = bigQuery;
    this.batchMaxRows = batchMaxRows;
    this.batchMaxSecondsElapsed = batchMaxSecondsElapsed;
    this.gcsWriter = new MultiGCSWriter(storage, bucket.getName(),
                                        String.format("cdap/delta/%s/", context.getApplicationName()));
    this.bucket = bucket;
    this.project = project;
    this.currentBatchSize = 0;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start() {
    scheduledFlush = executorService.scheduleAtFixedRate(() -> {
      try {
        flush();
      } catch (InterruptedException e) {
        // just return and let things end
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

  // TODO: handle errors
  @Override
  public void applyDDL(Sequenced<DDLEvent> sequencedEvent) {
    DDLEvent event = sequencedEvent.getEvent();
    switch (event.getOperation()) {
      case CREATE_DATABASE:
        DatasetId datasetId = DatasetId.of(project, event.getDatabase());
        if (bigQuery.getDataset(datasetId) == null) {
          DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).setLocation(bucket.getLocation()).build();
          bigQuery.create(datasetInfo);
        }
        break;
      case DROP_DATABASE:
        datasetId = DatasetId.of(project, event.getDatabase());
        if (bigQuery.getDataset(datasetId) != null) {
          bigQuery.delete(datasetId);
        }
        break;
      case CREATE_TABLE:
        TableId tableId = TableId.of(project, event.getDatabase(), event.getTable());
        Table table = bigQuery.getTable(tableId);
        // TODO: check schema of table if it exists already
        // TODO: add a way to get PK of a table through Delta API?
        if (table == null) {
          TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(Schemas.convert(addSequenceNumber(event.getSchema())))
            .build();
          String description;
          if (event.getPrimaryKey().isEmpty()) {
            description = "Primary Key: _sequence_num";
          } else {
            description = "Primary Key: " + event.getPrimaryKey().stream().collect(Collectors.joining(","));
          }
          TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition)
            .setDescription(description)
            .build();
          bigQuery.create(tableInfo);
        }
        break;
      case DROP_TABLE:
        tableId = TableId.of(project, event.getDatabase(), event.getTable());
        table = bigQuery.getTable(tableId);
        if (table != null) {
          bigQuery.delete(tableId);
        }
        break;
      case ALTER_TABLE:
        tableId = TableId.of(project, event.getDatabase(), event.getTable());
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
        break;
      case RENAME_TABLE:
        // TODO: create a copy job then delete previous table
        break;
      case TRUNCATE_TABLE:
        // TODO: run a DELETE from table WHERE 1=1 query
        break;
    }

  }

  private Schema addSequenceNumber(Schema original) {
    List<Schema.Field> fields = new ArrayList<>(original.getFields().size() + 1);
    fields.add(Schema.Field.of("_sequence_num", Schema.of(Schema.Type.LONG)));
    fields.addAll(original.getFields());
    return Schema.recordOf(original.getRecordName() + ".sequenced", fields);
  }

  @Override
  public void applyDML(Sequenced<DMLEvent> sequencedEvent) {
    gcsWriter.write(sequencedEvent);
    latestOffset = sequencedEvent.getEvent().getOffset();
    currentBatchSize++;
    if (currentBatchSize >= batchMaxRows) {
      try {
        flush();
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  // TODO: do this in parallel and asynchronously
  private synchronized void flush() throws InterruptedException {
    for (TableBlob blob : gcsWriter.flush()) {
      // [app name]_stage_[batch id]_[retry num].
      TableId stagingTableId = TableId.of(project, blob.getDataset(), "_staging_" + blob.getTable());
      Table stagingTable = bigQuery.getTable(stagingTableId);
      if (stagingTable == null) {
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
          .setLocation(bucket.getLocation())
          .setSchema(Schemas.convert(blob.getStagingSchema()))
          .build();
        TableInfo tableInfo = TableInfo.newBuilder(stagingTableId, tableDefinition).build();
        try {
          stagingTable = bigQuery.create(tableInfo);
        } catch (BigQueryException e) {
          // TODO: handler error
          LOG.error("Failed to create staging table {}", blob.getTable(), e);
          return;
        }
      }
      // TODO: verify schema of staging table

      // load data from GCS object into staging BQ table
      JobId jobId = JobId.newBuilder()
        .setLocation(bucket.getLocation())
        .setJob(String.format("%s_stage_%s_%s", context.getApplicationName(), blob.getTable(), blob.getBatchId()))
        .build();
      BlobId blobId = blob.getBlob().getBlobId();
      String uri = String.format("gs://%s/%s", blobId.getBucket(), blobId.getName());
      LoadJobConfiguration loadJobConf = LoadJobConfiguration.newBuilder(stagingTableId, uri)
        .setFormatOptions(FormatOptions.avro())
        .build();
      JobInfo jobInfo = JobInfo.newBuilder(loadJobConf)
        .setJobId(jobId)
        .build();
      try {
        // TODO: handle failures
        Job loadJob = bigQuery.create(jobInfo);
        loadJob.waitFor();
      } catch (BigQueryException e) {
        LOG.error("Failed to load data into BigQuery.", e);
        return;
      }

      // merge data from staging BQ table into target table
      TableId targetTableId = TableId.of(project, blob.getDataset(), blob.getTable());
      Table targetTable = bigQuery.getTable(targetTableId);
      if (targetTable == null) {
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
          .setLocation(bucket.getLocation())
          .setSchema(Schemas.convert(blob.getTargetSchema()))
          .build();
        TableInfo tableInfo = TableInfo.newBuilder(stagingTableId, tableDefinition).build();
        try {
          targetTable = bigQuery.create(tableInfo);
        } catch (BigQueryException e) {
          // TODO: handler error
          LOG.error("Failed to create target table {}", blob.getTable(), e);
          return;
        }
      }

      // TODO: figure app should provide a way to get table info given an offset
      String pkStr = targetTable.getDescription();
      pkStr = pkStr == null || !pkStr.startsWith("Primary Key: ") ?
        "_sequence_num" : pkStr.substring("Primary Key: ".length());
      List<String> primaryKey = Arrays.stream(pkStr.split(",")).collect(Collectors.toList());
      // TODO: verify schema of target table
      /*
       * SELECT S.* FROM
       *   (SELECT * FROM [dataset].[staging table] WHERE _batchId = [batch id]) as S
       *   JOIN (
       *     SELECT [primary key], MAX(_sequence_num) as maxSequenceNum
       *     FROM [dataset].[staging table]
       *     WHERE _batchId = [batch id]) as M
       *   ON [primary key equality] AND S._sequence_num = M.maxSequenceNum
       */
      String diffQuery = String.format(
        "SELECT S.* FROM "
          + "(SELECT * FROM %s.%s WHERE _batch_id = \"%s\") as S "
          + "JOIN ("
          + " SELECT %s, MAX(_sequence_num) as maxSequenceNum "
          + " FROM %s.%s "
          + " WHERE _batch_id = \"%s\" "
          + " GROUP BY %s) as M "
          + "ON %s AND S._sequence_num = M.maxSequenceNum",
        stagingTableId.getDataset(), stagingTableId.getTable(), blob.getBatchId(),
        pkStr,
        stagingTableId.getDataset(), stagingTableId.getTable(),
        blob.getBatchId(),
        pkStr,
        primaryKey.stream().map(k -> String.format("S.%s = M.%s", k, k)).collect(Collectors.joining(" AND ")));

      // D.column1 = T.column1 AND D.column2 = T.column2 ...
      String equalityStr = primaryKey.stream()
        .map(name -> String.format("D.%s = T.%s", name, name))
        .collect(Collectors.joining(" AND "));
      // column1 = S.column1, column2 = S.column2, ...
      String updateStr = blob.getTargetSchema().getFields().stream()
        .map(Schema.Field::getName)
        .map(name -> String.format("%s = D.%s", name, name))
        .collect(Collectors.joining(", "));
      String columns = blob.getTargetSchema().getFields().stream()
        .map(Schema.Field::getName)
        .collect(Collectors.joining(", "));
      String query = String.format(
        "MERGE %s.%s as T "
          + "USING (%s) as D ON %s AND D._sequence_num > T._sequence_num "
          + "WHEN MATCHED AND D._op = \"%s\" THEN DELETE "
          + "WHEN MATCHED AND D._op IN (\"%s\", \"%s\") THEN UPDATE SET %s "
          + "WHEN NOT MATCHED AND D._op IN (\"%s\", \"%s\") THEN INSERT (%s) VALUES (%s)",
        targetTableId.getDataset(), targetTableId.getTable(),
        diffQuery, equalityStr,
        DMLOperation.DELETE.name(),
        DMLOperation.INSERT.name(), DMLOperation.UPDATE.name(), updateStr,
        DMLOperation.INSERT.name(), DMLOperation.UPDATE.name(), columns, columns);
      QueryJobConfiguration mergeJobConf = QueryJobConfiguration.newBuilder(query).build();
      // TODO: make job ids unique -- right now they are not unique in failure conditions
      jobId = JobId.newBuilder()
        .setLocation(bucket.getLocation())
        .setJob(String.format("%s_merge_%s_%s", context.getApplicationName(), blob.getTable(), blob.getBatchId()))
        .build();
      jobInfo = JobInfo.newBuilder(mergeJobConf)
        .setJobId(jobId)
        .build();
      try {
        Job mergeJob = bigQuery.create(jobInfo);
        // TODO: handle failures
        mergeJob.waitFor();
      } catch (BigQueryException e) {
        LOG.error("Failed to run merge query", e);
        return;
      }

      blob.getBlob().delete();
    }
    currentBatchSize = 0;
    try {
      if (latestOffset != null) {
        context.commitOffset(latestOffset);
      }
    } catch (IOException e) {
      // TODO: retry failures
      LOG.error("Failed to commit offset", e);
    }
  }


}
