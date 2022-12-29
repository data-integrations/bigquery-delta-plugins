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
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.Sequenced;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@PrepareForTest({BigQueryEventConsumer.class, BigQueryUtils.class})
@RunWith(PowerMockRunner.class)
public class BigQueryConsumerTest {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryConsumerTest.class);
  private static final String TABLE_NAME_PREFIX = "table_";
  private static final String DATABASE = "database";
  private static final String DB_SCHEMA = "schema";
  private static final int LOAD_INTERVAL_SECONDS = 4;
  private static final String DATASET = "dataset";
  private static final String EMPTY_DATASET_NAME = "";
  private static final String BUCKET = "bucket";
  private static final String TABLE = "table";
  private static final int TABLE_COUNT = 5;
  private static final String PRIMARY_KEY_COL = "id";
  private static final int BQ_JOB_TIME_BOUND = 2;
  private static final int MAX_RETRY_SECONDS = 10;
  private static final Random random = new Random();
  private static final List<String> primaryKeys = Arrays.asList(PRIMARY_KEY_COL);
  private static final Schema schema = Schema.recordOf(TABLE,
                                                       Schema.Field.of(PRIMARY_KEY_COL, Schema.of(Schema.Type.INT)),
                                                       Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
  private static final BlobId blobId = BlobId.of(BUCKET, TABLE, 1L);

  @Mock
  private DeltaTargetContext deltaTargetContext;
  @Mock
  private Storage storage;
  @Mock
  private BigQuery bigQuery;
  @Mock
  private MultiGCSWriter gcsWriter;
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

  @Before
  public void setup() throws Exception {
    Mockito.when(deltaTargetContext.getMaxRetrySeconds()).thenReturn(MAX_RETRY_SECONDS);
    Mockito.when(bucket.getName()).thenReturn(BUCKET);
    Mockito.when(storage.writer(Mockito.any(BlobInfo.class))).thenReturn(writeChannel);
    Mockito.when(blob.getBlobId()).thenReturn(blobId);
    PowerMockito.whenNew(MultiGCSWriter.class).withAnyArguments().thenReturn(gcsWriter);

    Mockito.when(job.waitFor())
      .thenAnswer((a) -> {
        TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(BQ_JOB_TIME_BOUND));
        return job;
      });
    Mockito.when(job.getStatus()).thenReturn(Mockito.mock(JobStatus.class));
    Mockito.when(bigQuery.create(ArgumentMatchers.any(JobInfo.class))).thenReturn(job);
    Mockito.when(bigQuery.getTable(Mockito.any())).thenReturn(table);
    PowerMockito.spy(BigQueryUtils.class);
    PowerMockito.
      doReturn(0L)
      .when(BigQueryUtils.class, "getMaximumSequenceNumberForTable", ArgumentMatchers.eq(bigQuery),
            ArgumentMatchers.any(),
            ArgumentMatchers.any());
  }

  @Test
  public void testConsumerMultipleTableInsertEvents() throws Exception {
    List<String> tables = getTables(TABLE_COUNT);

    int numInsertEvents = 10;
    long batchId = 1L;
    setupMocksForGCSWriter(DATASET, tables, 10, batchId);

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_SECONDS, "_staging",
                                                                    true, null, 2L,
                                                                    DATASET, false);


    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents);

    //Wait for flush with some buffer
    Thread.sleep(TimeUnit.SECONDS.toMillis(LOAD_INTERVAL_SECONDS + 2));

    Mockito.verify(gcsWriter, Mockito.atLeastOnce()).flush();
    Mockito.verify(bigQuery, Mockito.times(1)).create(Mockito.any(DatasetInfo.class));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //Load and merge jobs
    Mockito.verify(bigQuery, Mockito.atLeast(TABLE_COUNT)).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(TABLE_COUNT)).delete(Mockito.any(TableId.class));

    LOG.info("Stopping eventConsumer");
    eventConsumer.stop();
    LOG.info("Stopped eventConsumer");

    //Clear existing interactions so that
    Mockito.reset(bigQuery, job, gcsWriter);

    LOG.info("Wait for load interval");
    //Let another round of load interval to pass so that we can verify
    //that no BQ jobs are fired after closing consumer
    Thread.sleep(TimeUnit.SECONDS.toMillis(LOAD_INTERVAL_SECONDS + 2));

    Mockito.verifyNoMoreInteractions(job, bigQuery, gcsWriter);
  }

  @Test
  public void testConsumerEmptyDataset() throws Exception {
    List<String> tables = getTables(1);

    int numInsertEvents = 10;
    long batchId = 1L;

    setupMocksForGCSWriter(DATABASE, tables, 10, batchId);

    BigQueryEventConsumer eventConsumer = new BigQueryEventConsumer(deltaTargetContext, storage,
                                                                    bigQuery, bucket, "project",
                                                                    LOAD_INTERVAL_SECONDS, "_staging",
                                                                    true, null, 2L,
                                                                    EMPTY_DATASET_NAME, false);
    eventConsumer.start();

    generateDDL(eventConsumer, tables);
    generateInsertEvents(eventConsumer, tables, numInsertEvents);

    //Wait for flush with some buffer
    Thread.sleep(TimeUnit.SECONDS.toMillis(LOAD_INTERVAL_SECONDS + 2));

    Mockito.verify(gcsWriter, Mockito.atLeastOnce()).flush();
    Mockito.verify(bigQuery, Mockito.times(1)).create(Mockito.any(DatasetInfo.class));
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).getTable(Mockito.any(TableId.class));
    //Mocks are setup such that the table already exists (for simplicity)
    Mockito.verify(bigQuery, Mockito.never()).create(Mockito.any(TableInfo.class));
    //Load and merge jobs
    Mockito.verify(bigQuery, Mockito.atLeastOnce()).create(Mockito.any(JobInfo.class));
    //Delete staging table
    Mockito.verify(bigQuery, Mockito.times(1)).delete(Mockito.any(TableId.class));

    eventConsumer.stop();
  }

  private int setupMocksForGCSWriter(String normalizedDatasetName, List<String> tables, int numInsertEvents,
                                     long batchId) throws IOException, InterruptedException {
    List<TableBlob> tableBlobs = new ArrayList<>();
    tables.forEach(table -> tableBlobs.add(createTableBlob(table, batchId, numInsertEvents, normalizedDatasetName)));
    Map<MultiGCSWriter.BlobType, Collection<TableBlob>> tableBlobsByBlobType = new HashMap<>();
    tableBlobsByBlobType.put(MultiGCSWriter.BlobType.STREAMING, tableBlobs);
    tableBlobsByBlobType.put(MultiGCSWriter.BlobType.SNAPSHOT, Collections.emptyList());
    Mockito.when(gcsWriter.flush()).thenReturn(tableBlobsByBlobType);
    return numInsertEvents;
  }

  private TableBlob createTableBlob(String table, long batchId, int numEvents, String dataset) {
    return new TableBlob(dataset, DB_SCHEMA, table, schema, schema, batchId, numEvents, blob, false, false);
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
                                    int numEvents) throws Exception {
    final AtomicInteger seq = new AtomicInteger(0);

    for (String tableName : tables) {
      for (int num = 0; num < numEvents; num++) {
        StructuredRecord record = StructuredRecord.builder(schema)
          .set(PRIMARY_KEY_COL, random.nextInt())
          .set("name", "alice")
          .build();

        DMLEvent insert1Event = DMLEvent.builder()
          .setOperationType(DMLOperation.Type.INSERT)
          .setIngestTimestamp(System.currentTimeMillis())
          .setSnapshot(false)
          .setDatabaseName(DATABASE)
          .setTableName(tableName)
          .setRow(record)
          .setOffset(new Offset())
          .build();
        eventConsumer.applyDML(new Sequenced<>(insert1Event, seq.incrementAndGet()));
      }
    }
  }
}
