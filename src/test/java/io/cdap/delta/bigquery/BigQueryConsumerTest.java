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

import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.Sequenced;
import org.apache.avro.file.DataFileWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@PrepareForTest({AvroEventWriter.class})
@RunWith(PowerMockRunner.class)
public class BigQueryConsumerTest {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryConsumerTest.class);
  private static final String TABLE_NAME_PREFIX = "table_";
  private static final String DATABASE = "database";
  private static final int LOAD_INTERVAL_SECONDS = 4;
  private static final int LOAD_INTERVAL_ONE_SECOND = 1;
  private static final String DATASET = "dataset";
  private static final String EMPTY_DATASET_NAME = "";
  private static final String BUCKET = "bucket";
  private static final String TABLE = "table";
  private static final String PRIMARY_KEY_COL = "id";
  private static final String NAME_COL = "name";
  private static final String MERGE_JOB = "merge";
  private static final long GENERATION = 1L;
  private static final int BQ_JOB_TIME_BOUND = 2;
  private static final int MAX_RETRY_SECONDS = 10;
  private static final Random random = new Random();
  private static final List<String> primaryKeys = Arrays.asList(PRIMARY_KEY_COL);
  private static final Schema schema = Schema.recordOf(TABLE,
                                                       Schema.Field.of(PRIMARY_KEY_COL, Schema.of(Schema.Type.INT)),
                                                       Schema.Field.of(NAME_COL, Schema.of(Schema.Type.STRING)));
  private static final BlobId blobId = BlobId.of(BUCKET, TABLE, GENERATION);
  private static final int WAIT_BUFFER_SEC = 2;
  private static final boolean CDC = false;
  private static final boolean SNAPSHOT = true;

  @Mock
  private DeltaTargetContext deltaTargetContext;
  @Mock
  private Storage storage;
  @Mock
  private BigQuery bigQuery;
  @Mock
  private Bucket bucket;
  @Mock
  private WriteChannel writeChannel;
  @Mock
  private Blob blob;
  @Mock
  private Table table;
  @Mock
  private Job job;
  @Mock
  private DataFileWriter dataFileWriter;
  @Rule
  private ExpectedException exceptionRule = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    Mockito.when(deltaTargetContext.getMaxRetrySeconds()).thenReturn(MAX_RETRY_SECONDS).getMock();
    Mockito.when(bucket.getName()).thenReturn(BUCKET);
    Mockito.when(storage.writer(Mockito.any(BlobInfo.class))).thenReturn(writeChannel);
    Mockito.when(storage.get(Mockito.any(BlobId.class))).thenReturn(blob);
    Mockito.when(blob.getBlobId()).thenReturn(blobId);
    Mockito.when(dataFileWriter.create(Mockito.any(org.apache.avro.Schema.class), Mockito.any(OutputStream.class)))
      .thenReturn(dataFileWriter);
    PowerMockito.whenNew(DataFileWriter.class).withAnyArguments().thenReturn(dataFileWriter);


    //Random execution time for BigQuery job
    Mockito.when(job.waitFor())
      .thenAnswer((a) -> {
        TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(BQ_JOB_TIME_BOUND));
        return job;
      });
    Mockito.when(job.getStatus()).thenReturn(Mockito.mock(JobStatus.class));
    Mockito.when(bigQuery.create(ArgumentMatchers.any(JobInfo.class))).thenReturn(job);
    Mockito.when(bigQuery.getTable(Mockito.any())).thenReturn(table);

    //mocks for BigQueryUtils.getMaximumSequenceNumberForTable
    TableResult tableResult = Mockito.mock(TableResult.class);
    Mockito.when(tableResult.iterateAll()).thenReturn(Collections.emptyList());
    Mockito.when(job.getQueryResults()).thenReturn(tableResult);
  }

  @Test
  public void testConsumerMultipleTableInsertEvents() throws Exception {
    int numTables = 10;
    int numInsertEvents = 10;

    List<String> tables = getTables(numTables);

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_SECONDS, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_SECONDS, 1);

    Mockito.verify(dataFileWriter, Mockito.times(numTables * numInsertEvents))
      .append(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.atLeast(numTables)).close();
    Mockito.verify(bigQuery, Mockito.times(1)).create(datasetIs(DATASET));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //Load and merge jobs
    Mockito.verify(bigQuery, Mockito.atLeast(numTables)).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(numTables)).delete(Mockito.any(TableId.class));

    LOG.info("Stopping eventConsumer");
    eventConsumer.stop();
    LOG.info("Stopped eventConsumer");

    //Clear existing interactions
    Mockito.reset(bigQuery, job);

    LOG.info("Wait for load interval");
    //Let another round of load interval to pass so that we can verify
    //that no BQ jobs are fired after closing consumer
    waitForFlushWithBuffer(LOAD_INTERVAL_SECONDS, 1);

    Mockito.verifyNoMoreInteractions(job, bigQuery);
  }

  @Test
  public void testConsumerEmptyDataset() throws Exception {
    int numTables = 1;
    int numInsertEvents = 10;

    List<String> tables = getTables(numTables);

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_SECONDS, "_staging",
                                                                    false, null, 2L,
                                                                    EMPTY_DATASET_NAME, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_SECONDS, 1);

    Mockito.verify(dataFileWriter, Mockito.times(numInsertEvents)).append(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.atLeast(numTables)).close();
    Mockito.verify(bigQuery, Mockito.times(1)).create(datasetIs(DATABASE));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //Load and merge jobs
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(numTables)).delete(Mockito.any(TableId.class));

    eventConsumer.stop();
  }

  @Test
  public void testMergeMultipleTimesUsesPrimaryKeyCache() throws Exception {
    int numTables = 1;
    int numInsertEvents = 10;
    List<String> tables = getTables(numTables);

    Mockito.when(deltaTargetContext.getState(Mockito.matches("bigquery-.*")))
      .thenReturn(GSON.toJson(new BigQueryTableState(Arrays.asList(PRIMARY_KEY_COL))).getBytes());

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    EMPTY_DATASET_NAME, false);
    eventConsumer.start();

    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_ONE_SECOND, 1);

    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC, 10);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_ONE_SECOND, 1);

    //Verify primary keys were fetched only once from state store and then cached
    Mockito.verify(deltaTargetContext, Mockito.times(1)).getState(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.times(2)).close();
    //Mocks are setup such that the table already exists (for simplicity)
    //max sequence num, load and merge jobs
    Mockito.verify(bigQuery, Mockito.atLeast(3)).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(2)).delete(Mockito.any(TableId.class));

    eventConsumer.stop();
  }

  public void testConsumerCommitFailureRetries() throws Exception {
    int numTables = 1;
    int numInsertEvents = 5;

    List<String> tables = getTables(numTables);

    Throwable error = new Throwable("network error");
    SuccessAfterNFailures answer = new SuccessAfterNFailures(3, error);
    Mockito.doAnswer(answer)
      .when(deltaTargetContext)
      .commitOffset(Mockito.any(Offset.class), Mockito.anyLong());

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_ONE_SECOND, 10);

    Mockito.verify(dataFileWriter, Mockito.times(numTables * numInsertEvents))
      .append(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.atLeast(numTables)).close();
    Mockito.verify(bigQuery, Mockito.times(1)).create(datasetIs(DATASET));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //Load and merge jobs
    Mockito.verify(bigQuery, Mockito.atLeast(numTables)).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(numTables)).delete(Mockito.any(TableId.class));
    //Verify there were 3 retries
    Mockito.verify(deltaTargetContext, Mockito.atLeast(3))
      .commitOffset(Mockito.any(Offset.class), Mockito.anyLong());

    eventConsumer.stop();
  }

  @Test
  public void testGcsWriteInMemoryFailureRetries() throws Exception {
    Mockito.doThrow(new IllegalStateException()).when(dataFileWriter).append(Mockito.any());

    StructuredRecord record = StructuredRecord.builder(schema)
      .set(PRIMARY_KEY_COL, random.nextInt())
      .set(NAME_COL, "alice")
      .build();

    DMLEvent insert1Event = DMLEvent.builder()
      .setOperationType(DMLOperation.Type.INSERT)
      .setDatabaseName(DATABASE)
      .setTableName(TABLE)
      .setRow(record)
      .build();

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);

    try {
      exceptionRule.expect(IllegalStateException.class);
      eventConsumer.applyDML(new Sequenced<>(insert1Event, 1));
    } finally {
      //Verify that retry happens
      Mockito.verify(dataFileWriter, Mockito.atLeast(2)).append(Mockito.any());
      eventConsumer.stop();
    }
  }


  /**
   *  Test checks retry handling in case of create and get status failure for load job
   *
   *  First attempt - failure on job wait call after it was created
   *  Second attempt - previous attempt is found, but again failure in job wait
   *  Third attempt - Job for second attempt is not found, but job for first attempt is found
   *                  which was successful
   *
   *
   * @throws Exception
   */
  @Test
  public void testLoadJobRetriesCheckPreviousAttemptStatus() throws Exception {
    int numTables = 1;
    int numInsertEvents = 5;

    List<String> tables = getTables(numTables);

    Throwable error = new Throwable("network error");
    Job waitForFailure = Mockito.mock(Job.class);
    Mockito.when(waitForFailure.waitFor(Mockito.any())).thenThrow(new BigQueryException(403, "error", error));

    Mockito.when(bigQuery.create(isJobType(JobConfiguration.Type.LOAD), Mockito.any()))
      .thenReturn(waitForFailure);

    Mockito
      .doReturn(waitForFailure)
      .doReturn(job)
      .when(bigQuery).getJob(isForAttempt(0));

    Mockito
      .doReturn(null)
      .when(bigQuery).getJob(isForAttempt(1));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_ONE_SECOND, 10);

    Mockito.verify(dataFileWriter, Mockito.times(numTables * numInsertEvents))
      .append(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.atLeast(numTables)).close();
    Mockito.verify(bigQuery, Mockito.times(1)).create(datasetIs(DATASET));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //1 query(find max seq num), 1 Load, 1 merge jobs
    Mockito.verify(bigQuery, Mockito.times(3)).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(numTables)).delete(Mockito.any(TableId.class));

    eventConsumer.stop();
  }

  /**
   * Test checks retry handling in case of get status failure for load job which is in error state
   *
   *  First attempt - failure on job wait call after it was created
   *  Second attempt - previous attempt is found, but it was in failed state so new job is created and is successful
   *
   * @throws Exception
   */
  @Test
  public void testLoadJobRetriesCheckPreviousAttemptWasFailed() throws Exception {
    int numTables = 1;
    int numInsertEvents = 5;

    List<String> tables = getTables(numTables);

    Throwable error = new Throwable("network error");

    Job waitForFailureJob = Mockito.mock(Job.class);
    Mockito.when(waitForFailureJob.waitFor(Mockito.any())).thenThrow(new BigQueryException(403, "error", error));

    Job errorJob = Mockito.mock(Job.class);
    Mockito.when(errorJob.getStatus()).thenReturn(Mockito.mock(JobStatus.class));
    Mockito.when(errorJob.getStatus().getState()).thenReturn(JobStatus.State.DONE);
    Mockito.when(errorJob.getStatus().getError()).thenReturn(new BigQueryError("reason", "location", "error"));

    Mockito.when(bigQuery.create(isJobType(JobConfiguration.Type.LOAD), Mockito.any()))
      .thenReturn(waitForFailureJob)
      .thenReturn(job);

    Mockito
      .doReturn(waitForFailureJob)
      .doReturn(errorJob)
      .when(bigQuery).getJob(isForAttempt(0));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_ONE_SECOND, 10);

    Mockito.verify(dataFileWriter, Mockito.times(numTables * numInsertEvents))
      .append(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.atLeast(numTables)).close();
    Mockito.verify(bigQuery, Mockito.times(1)).create(datasetIs(DATASET));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //1 query(find max seq num), 2 Load, 1 merge jobs
    Mockito.verify(bigQuery, Mockito.times(4)).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(numTables)).delete(Mockito.any(TableId.class));

    eventConsumer.stop();
  }

  /**
   *  Test checks retry handling in case of create and get status failure for load job
   *
   *  First attempt - failure on job wait call after it was created
   *  Second attempt - previous attempt is found, but again failure in job wait
   *  Third attempt - Job for second attempt is not found, but job for first attempt is found
   *                  which was successful
   *
   *
   * @throws Exception
   */
  @Test
  public void testMergeJobRetriesCheckPreviousAttemptStatus() throws Exception {
    int numTables = 1;
    int numInsertEvents = 5;

    List<String> tables = getTables(numTables);

    Throwable error = new Throwable("network error");
    Job waitForFailure = Mockito.mock(Job.class);
    Mockito.when(waitForFailure.waitFor(Mockito.any())).thenThrow(new BigQueryException(403, "error", error));

    Mockito.when(bigQuery.create(isJobTypeAndCategory(JobConfiguration.Type.QUERY, MERGE_JOB), Mockito.any()))
      .thenReturn(waitForFailure);

    Mockito
      .doReturn(waitForFailure)
      .doReturn(job)
      .when(bigQuery).getJob(isForAttempt(0));

    Mockito
      .doReturn(null)
      .when(bigQuery).getJob(isForAttempt(1));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_ONE_SECOND, 10);

    Mockito.verify(dataFileWriter, Mockito.times(numTables * numInsertEvents))
      .append(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.atLeast(numTables)).close();
    Mockito.verify(bigQuery, Mockito.times(1)).create(datasetIs(DATASET));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //1 query(find max seq num), 1 Load, 1 merge jobs
    Mockito.verify(bigQuery, Mockito.times(3)).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(numTables)).delete(Mockito.any(TableId.class));

    eventConsumer.stop();
  }

  /**
   *  Test checks retry handling in case of get status failure for merge job which is in error state
   *
   *   First attempt - failure on job wait call after it was created
   *   Second attempt - previous attempt is found, but it was in failed state so new job is created and is
   *                    successful
   *
   * @throws Exception
   */
  @Test
  public void testMergeJobRetriesCheckPreviousAttemptWasFailed() throws Exception {
    int numTables = 1;
    int numInsertEvents = 5;

    List<String> tables = getTables(numTables);

    Throwable error = new Throwable("network error");

    Job waitForFailureJob = Mockito.mock(Job.class);
    Mockito.when(waitForFailureJob.waitFor(Mockito.any())).thenThrow(new BigQueryException(403, "error", error));

    Job errorJob = Mockito.mock(Job.class);
    Mockito.when(errorJob.getStatus()).thenReturn(Mockito.mock(JobStatus.class));
    Mockito.when(errorJob.getStatus().getState()).thenReturn(JobStatus.State.DONE);
    Mockito.when(errorJob.getStatus().getError()).thenReturn(new BigQueryError("reason", "location", "error"));

    Mockito.when(bigQuery.create(isJobTypeAndCategory(JobConfiguration.Type.QUERY, MERGE_JOB), Mockito.any()))
      .thenReturn(waitForFailureJob)
      .thenReturn(job);

    Mockito
      .doReturn(errorJob)
      .when(bigQuery).getJob(isForAttempt(0));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents, CDC);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_ONE_SECOND, 4);

    Mockito.verify(dataFileWriter, Mockito.times(numTables * numInsertEvents))
      .append(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.atLeast(numTables)).close();
    Mockito.verify(bigQuery, Mockito.times(1)).create(datasetIs(DATASET));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //1 query(find max seq num), 1 Load, 2 merge jobs
    Mockito.verify(bigQuery, Mockito.times(4)).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(numTables)).delete(Mockito.any(TableId.class));

    eventConsumer.stop();
  }

  /**
   * Validates duplicates events are not inserted to target table in following scenario
   *  1. Snapshot events with seq no 1-100 are inserted in target table
   *  2. Before commiting offset, there is a worker crash
   *  3. On worker restart, source plugin resumes from last saved offset (it  supports resuming snapshot)
   *  4. Events with seq no 1-100 are re-generated by source plugin
   *  5. Target plugin ensures that events with sequence number lower than max sequence number
   *     in the target table are dropped
   * @throws Exception
   */
  @Test
  public void testDataConsistencyInSnapshotEventReplay() throws Exception {
    int numTables = 2;
    int numInsertEvents = 5;

    List<String> tables = getTables(numTables);

    //max sequence num in target table is 10
    TableResult tableResult = Mockito.mock(TableResult.class);
    FieldValue maxSeq = FieldValue.of(FieldValue.Attribute.PRIMITIVE, "10");
    FieldValueList fieldValueList = FieldValueList
      .of(Arrays.asList(maxSeq), Field.of("max", StandardSQLTypeName.NUMERIC));
    Mockito.when(tableResult.iterateAll()).thenReturn(Arrays.asList(fieldValueList));
    Mockito.when(job.getQueryResults()).thenReturn(tableResult);

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    //Generates event with seq num <= 10 (2 tables 5 events each starting from 1)
    generateInsertEvents(eventConsumer, tables, numInsertEvents, SNAPSHOT);

    //Wait for flush with some buffer
    waitForFlushWithBuffer(LOAD_INTERVAL_ONE_SECOND, 1);

    //Verify that replay events were not written
    Mockito.verify(dataFileWriter, Mockito.never()).append(Mockito.any());
    Mockito.verify(dataFileWriter, Mockito.never()).close();

    Mockito.verify(bigQuery, Mockito.times(1)).create(datasetIs(DATASET));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Fetch sequence num (x2 for 2 tables), no load jobs as all events are filtered out
    Mockito.verify(bigQuery, Mockito.times(2)).create(Mockito.any(JobInfo.class));
    eventConsumer.stop();
  }

  @Test
  public void testPermanentFailureIsNotRetriedInProcessDDL() throws Exception {
    int numTables = 1;
    List<String> tables = getTables(numTables);

    Mockito.when(bigQuery.getTable(Mockito.any())).thenReturn(null);
    BigQueryError error = new BigQueryError("invalid", "loc", "error");
    Mockito.when(bigQuery.create(Mockito.any(TableInfo.class)))
      .thenThrow(new BigQueryException(400, "error", error));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();
    try {
      exceptionRule.expect(DeltaFailureException.class);
      generateDDL(eventConsumer, tables);
    } finally {
      Mockito.verify(bigQuery, Mockito.times(1)).create(Mockito.any(TableInfo.class));
      eventConsumer.stop();
    }
  }

  @Test
  public void testTemporaryFailureIsRetriedInProcessDDL() throws Exception {
    int numTables = 1;
    List<String> tables = getTables(numTables);

    Mockito.when(bigQuery.getTable(Mockito.any())).thenReturn(null);
    BigQueryError error = new BigQueryError("ratelimit", "loc", "error");
    Mockito.when(bigQuery.create(Mockito.any(TableInfo.class)))
      .thenThrow(new BigQueryException(429, "error", error));

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_ONE_SECOND, "_staging",
                                                                    false, null, 2L,
                                                                    DATASET, false);
    eventConsumer.start();

    ExecutorService executorService = Executors.newFixedThreadPool(1);

    try {
      List<Future<Object>> futures = executorService.invokeAll(Arrays.asList(() -> {
        generateDDL(eventConsumer, tables);
        return null;
      }), 5, TimeUnit.SECONDS);

      for (Future<Object> future : futures) {
        future.get();
      }
    } catch (Exception e) {
      //Task was forcefully cancelled as it was still in retry
      Assert.assertEquals(CancellationException.class, e.getClass());
    } finally {
      eventConsumer.stop();
      executorService.shutdownNow();
      Mockito.verify(bigQuery, Mockito.atLeast(2)).create(Mockito.any(TableInfo.class));
    }
  }

  private JobId isForAttempt(int i) {
    return Mockito.argThat(jobId -> jobId.getJob().endsWith("_" + i));
  }

  private void waitForFlushWithBuffer(int loadIntervalSeconds, int flushCount) throws InterruptedException {
    Thread.sleep(TimeUnit.SECONDS.toMillis(flushCount * (loadIntervalSeconds + WAIT_BUFFER_SEC)));
  }

  private JobInfo isJobType(JobConfiguration.Type jobType) {
    return Mockito.argThat(jobInfo -> jobInfo.getConfiguration().getType() == jobType);
  }

  private JobInfo isJobTypeAndCategory(JobConfiguration.Type jobType, String category) {
    return Mockito.argThat(
      jobInfo -> jobInfo.getConfiguration().getType() == jobType
        && jobInfo.getJobId().getJob().contains(category));
  }

  private DatasetInfo datasetIs(String database) {
    return Mockito.argThat(datasetInfo -> datasetInfo.getDatasetId().getDataset().equals(database));
  }

  private List<String> getTables(int n) {
    return IntStream.range(0, n).mapToObj(i -> TABLE_NAME_PREFIX + i).collect(Collectors.toList());
  }

  private void generateDDL(BigQueryEventConsumer eventConsumer, List<String> tables) throws Exception {
    DDLEvent createDatabase = DDLEvent.builder()
      .setOperation(DDLOperation.Type.CREATE_DATABASE)
      .setDatabaseName(DATABASE)
      .setOffset(new Offset())
      .build();

    eventConsumer.applyDDL(new Sequenced<>(createDatabase, 0));

    for (String table : tables) {
      DDLEvent createTable = DDLEvent.builder()
        .setOperation(DDLOperation.Type.CREATE_TABLE)
        .setDatabaseName(DATABASE)
        .setTableName(table)
        .setSchema(schema)
        .setPrimaryKey(primaryKeys)
        .setOffset(new Offset())
        .build();
      eventConsumer.applyDDL(new Sequenced<>(createTable, 0));
    }
  }

  private void generateInsertEvents(BigQueryEventConsumer eventConsumer, List<String> tables,
                                    int numEvents, boolean isSnapshot) throws Exception {
    generateInsertEvents(eventConsumer, tables, numEvents, isSnapshot, 0);
  }
  private void generateInsertEvents(BigQueryEventConsumer eventConsumer, List<String> tables,
                                    int numEvents, boolean isSnapshot, Integer seqNum) throws Exception {
    final AtomicInteger seq = new AtomicInteger(seqNum);

    for (String tableName : tables) {
      for (int num = 0; num < numEvents; num++) {
        StructuredRecord record = StructuredRecord.builder(schema)
          .set(PRIMARY_KEY_COL, random.nextInt())
          .set(NAME_COL, "alice")
          .build();

        DMLEvent insert1Event = DMLEvent.builder()
          .setOperationType(DMLOperation.Type.INSERT)
          .setIngestTimestamp(System.currentTimeMillis())
          .setSnapshot(isSnapshot)
          .setDatabaseName(DATABASE)
          .setTableName(tableName)
          .setRow(record)
          .setOffset(new Offset())
          .build();
        eventConsumer.applyDML(new Sequenced<>(insert1Event, seq.incrementAndGet()));
      }
    }
  }

  static class SuccessAfterNFailures implements Answer<Void> {
    private final int totalFailures;
    private final Throwable error;
    private AtomicInteger currFailures;

    SuccessAfterNFailures(int totalFailures, Throwable error) {
      this.error = error;
      this.currFailures = new AtomicInteger();
      this.totalFailures = totalFailures;
    }

    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable {
      if (currFailures.incrementAndGet() <= totalFailures) {
        throw error;
      }
      return null;
    }
  }
}
