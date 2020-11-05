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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.Sequenced;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Writes to multiple GCS files.
 */
public class MultiGCSWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MultiGCSWriter.class);
  private final Storage storage;
  private final String bucket;
  private final String baseObjectName;
  private final Map<Key, TableObject> objects;
  private final Map<Schema, org.apache.avro.Schema> schemaMap;
  private final DeltaTargetContext context;
  private final ExecutorService executorService;

  public MultiGCSWriter(Storage storage, String bucket, String baseObjectName,
                        DeltaTargetContext context, ExecutorService executorService) {
    this.storage = storage;
    this.bucket = bucket;
    this.baseObjectName = baseObjectName;
    this.objects = new HashMap<>();
    this.schemaMap = new HashMap<>();
    this.context = context;
    this.executorService = executorService;
  }

  public synchronized void write(Sequenced<DMLEvent> sequencedEvent) {
    DMLEvent event = sequencedEvent.getEvent();
    DMLOperation dmlOperation = event.getOperation();
    Key key = new Key(event.getDatabase(), dmlOperation.getTableName());
    TableObject tableObject = objects.computeIfAbsent(key, t -> new TableObject(event.getDatabase(),
                                                                                dmlOperation.getTableName()));
    try {
      tableObject.writeEvent(sequencedEvent);
    } catch (IOException e) {
      // this should never happen, as it's writing to an in memory byte[]
      throw new IllegalStateException(String.format("Unable to write event %s to bytes.", event), e);
    }
  }

  public synchronized Collection<TableBlob> flush() throws IOException, InterruptedException {
    List<TableBlob> writtenObjects = new ArrayList<>(objects.size());
    List<Future<TableBlob>> writeFutures = new ArrayList<>(objects.size());
    for (Map.Entry<Key, TableObject> entry : objects.entrySet()) {
      TableObject tableObject = entry.getValue();
      writeFutures.add(executorService.submit(() -> {
        LOG.debug("Writing batch {} of {} events into GCS for table {}.{}", tableObject.batchId, tableObject.numEvents,
                  tableObject.dataset, tableObject.table);
        try {
          tableObject.close();
        } catch (IOException e) {
          String errMsg = String.format("Error writing batch of %d changes for %s.%s to GCS",
                                        tableObject.numEvents, tableObject.dataset, tableObject.table);
          context.setTableError(tableObject.dataset, tableObject.table,
                                new ReplicationError(errMsg, e.getStackTrace()));
          throw new IOException(errMsg, e);
        }
        LOG.debug("Wrote batch {} of {} events into GCS for table {}.{}", tableObject.batchId, tableObject.numEvents,
                  tableObject.dataset, tableObject.table);

        Blob blob = storage.get(tableObject.blobId);
        return new TableBlob(tableObject.dataset, tableObject.table, tableObject.targetSchema,
                             tableObject.stagingSchema, tableObject.batchId, tableObject.numEvents, blob);
      }));
    }

    IOException error = null;
    for (Future<TableBlob> writeFuture : writeFutures) {
      try {
        writtenObjects.add(getWriteFuture(writeFuture));
      } catch (InterruptedException e) {
        throw e;
      } catch (IOException e) {
        if (error == null) {
          error = e;
        } else {
          error.addSuppressed(e);
        }
      }
    }

    if (error != null) {
      throw error;
    }
    objects.clear();
    return writtenObjects;
  }

  private static TableBlob getWriteFuture(Future<TableBlob> writeFuture) throws IOException, InterruptedException {
    try {
      return writeFuture.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      }
      throw new RuntimeException(cause.getMessage(), cause);
    }
  }

  private class Key {
    private final String database;
    private final String table;

    private Key(String database, String table) {
      this.database = database;
      this.table = table;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return Objects.equals(database, key.database) &&
        Objects.equals(table, key.table);
    }

    @Override
    public int hashCode() {
      return Objects.hash(database, table);
    }
  }

  /**
   * Content to write to a GCS Object for a BigQuery table.
   */
  private class TableObject {
    private final OutputStream outputStream;
    private final long batchId;
    private final String dataset;
    private final String table;
    private final BlobId blobId;
    private int numEvents;
    private Schema stagingSchema;
    private Schema targetSchema;
    private DataFileWriter<StructuredRecord> avroWriter;

    private TableObject(String dataset, String table) {
      this.dataset = dataset;
      this.table = table;
      this.numEvents = 0;
      batchId = System.currentTimeMillis();
      String objectName = String.format("%s%s/%s/%d", baseObjectName, dataset, table, batchId);
      blobId = BlobId.of(bucket, objectName);
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      outputStream = Channels.newOutputStream(storage.writer(blobInfo));
    }

    private void writeEvent(Sequenced<DMLEvent> sequencedEvent) throws IOException {
      DMLEvent event = sequencedEvent.getEvent();
      StructuredRecord row = event.getRow();
      if (avroWriter == null) {
        targetSchema = getTargetSchema(row.getSchema());
        stagingSchema = getStagingSchema(targetSchema);
        DatumWriter<StructuredRecord> datumWriter = new RecordDatumWriter();
        org.apache.avro.Schema avroSchema = schemaMap.computeIfAbsent(stagingSchema, s -> {
          org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
          return parser.parse(s.toString());
        });
        avroWriter = new DataFileWriter<>(datumWriter).create(avroSchema, outputStream);
      }

      avroWriter.append(createStagingRecord(sequencedEvent));
      numEvents++;
    }

    private void close() throws IOException {
      if (avroWriter != null) {
        try {
          avroWriter.close();
        } finally {
          outputStream.close();
        }
      } else {
        outputStream.close();
      }
    }

    private Schema getTargetSchema(Schema schema) {
      List<Schema.Field> fields = new ArrayList<>(schema.getFields().size() + 1);
      fields.addAll(schema.getFields());
      fields.add(Schema.Field.of("_sequence_num", Schema.of(Schema.Type.LONG)));
      return Schema.recordOf(schema.getRecordName() + ".target", fields);
    }

    /*
        The staging table contains information about the batch id, sequence number, operation type,
        the previous value, and the new value. Previous value is null except for update events.

        For example, suppose the source table has two columns -- id (long) and name (string).
        The staging table would have the following columns:

          _op (string)
          _batch_id (long)
          _sequence_num (long)
          id (long)
          name (string)
          _before_id (long)
          _before_name (string)
     */
    private Schema getStagingSchema(Schema schema) {
      List<Schema.Field> fields = new ArrayList<>(2 * schema.getFields().size() + 3);
      fields.add(Schema.Field.of("_op", Schema.of(Schema.Type.STRING)));
      fields.add(Schema.Field.of("_batch_id", Schema.of(Schema.Type.LONG)));
      fields.addAll(schema.getFields());
      for (Schema.Field field : schema.getFields()) {
        if ("_sequence_num".equals(field.getName())) {
          continue;
        }
        String beforeName = "_before_" + field.getName();
        Schema beforeSchema = field.getSchema();
        beforeSchema = beforeSchema.isNullable() ? beforeSchema : Schema.nullableOf(beforeSchema);
        fields.add(Schema.Field.of(beforeName, beforeSchema));
      }
      return Schema.recordOf(schema.getRecordName() + ".staging", fields);
    }

    private StructuredRecord createStagingRecord(Sequenced<DMLEvent> sequencedEvent) {
      DMLEvent event = sequencedEvent.getEvent();
      StructuredRecord row = event.getRow();
      StructuredRecord.Builder builder = StructuredRecord.builder(stagingSchema);
      builder.set("_op", event.getOperation().getType().name());
      builder.set("_batch_id", batchId);
      builder.set("_sequence_num", sequencedEvent.getSequenceNumber());
      for (Schema.Field field : row.getSchema().getFields()) {
        builder.set(field.getName(), row.get(field.getName()));
      }

      StructuredRecord beforeRow = null;
      switch (event.getOperation().getType()) {
        case UPDATE:
          beforeRow = event.getPreviousRow();
          if (beforeRow == null) {
            // should never happen unless the source is implemented incorrectly
            throw new IllegalStateException(String.format(
              "Encountered an update event for %s.%s that did not include the previous column values. "
                + "Previous column values are required for replication.",
              event.getDatabase(), event.getOperation().getTableName()));
          }
          break;
        case DELETE:
          beforeRow = row;
          break;
      }
      if (beforeRow != null) {
        for (Schema.Field field : beforeRow.getSchema().getFields()) {
          builder.set("_before_" + field.getName(), beforeRow.get(field.getName()));
        }
      }
      return builder.build();
    }
  }
}
