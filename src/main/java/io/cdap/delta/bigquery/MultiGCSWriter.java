/*
 * Copyright © 2019-2020 Cask Data, Inc.
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
import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaTargetContext;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.Sequenced;
import io.cdap.delta.api.SourceProperties;
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
import javax.annotation.Nullable;

/**
 * Writes to multiple GCS files.
 */
public class MultiGCSWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MultiGCSWriter.class);
  private static final Gson GSON = new Gson();
  private final Storage storage;
  private final String bucket;
  private final String baseObjectName;
  private final Map<Key, TableObject> objects;
  private final Map<Schema, org.apache.avro.Schema> schemaMap;
  private final DeltaTargetContext context;
  private final ExecutorService executorService;
  private final boolean rowIdSupported;
  private final SourceProperties.Ordering eventOrdering;

  /**
   * GCS blob type can be SNAPSHOT when all records in the blob represents snapshot or STREAMING
   * when all records in the blob are part of streaming.
   */
  public enum BlobType {
    SNAPSHOT,
    STREAMING
  }

  public MultiGCSWriter(Storage storage, String bucket, String baseObjectName,
                        DeltaTargetContext context, ExecutorService executorService) {
    this.storage = storage;
    this.bucket = bucket;
    this.baseObjectName = baseObjectName;
    this.objects = new HashMap<>();
    this.schemaMap = new HashMap<>();
    this.context = context;
    this.executorService = executorService;
    this.rowIdSupported = context.getSourceProperties() != null && context.getSourceProperties().isRowIdSupported();
    this.eventOrdering = context.getSourceProperties() == null ? SourceProperties.Ordering.ORDERED :
      context.getSourceProperties().getOrdering();
  }

  public synchronized void write(Sequenced<DMLEvent> sequencedEvent) {
    DMLEvent event = sequencedEvent.getEvent();
    DMLOperation dmlOperation = event.getOperation();
    Key key = new Key(event.getOperation().getDatabaseName(), dmlOperation.getTableName(), event.isSnapshot());
    TableObject tableObject = objects.computeIfAbsent(key, t -> new TableObject(dmlOperation.getDatabaseName(),
                                                                                dmlOperation.getSchemaName(),
                                                                                dmlOperation.getTableName(),
                                                                                event.isSnapshot(),
                                                                                isJsonFormat(event.getRow())));
    try {
      tableObject.writeEvent(sequencedEvent);
    } catch (IOException e) {
      // this should never happen, as it's writing to an in memory byte[]
      throw new IllegalStateException(String.format("Unable to write event %s to bytes.", event), e);
    }
  }

  private boolean isJsonFormat(StructuredRecord record) {
    List<Schema.Field> fields = record.getSchema().getFields();
    if (fields == null) {
      return false;
    }
    for (Schema.Field field : fields) {
      boolean isNullable = field.getSchema().isNullable();
      Schema.LogicalType logicalType
        = isNullable ? field.getSchema().getNonNullable().getLogicalType() : field.getSchema().getLogicalType();
      if (logicalType != null && logicalType.equals(Schema.LogicalType.DATETIME)) {
        return true;
      }
    }
    return false;
  }

  public synchronized Map<BlobType, Collection<TableBlob>> flush() throws IOException, InterruptedException {
    List<Future<TableBlob>> writeFutures = new ArrayList<>(objects.size());
    Map<BlobType, Collection<TableBlob>> result = new HashMap<>();
    result.put(BlobType.SNAPSHOT, new ArrayList<>());
    result.put(BlobType.STREAMING, new ArrayList<>());
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
          context.setTableError(tableObject.dataset, tableObject.sourceDbSchemaName, tableObject.table,
                                new ReplicationError(errMsg, e.getStackTrace()));
          throw new IOException(errMsg, e);
        }
        LOG.debug("Wrote batch {} of {} events into GCS for table {}.{}", tableObject.batchId, tableObject.numEvents,
                  tableObject.dataset, tableObject.table);

        Blob blob = storage.get(tableObject.blobId);
        return new TableBlob(tableObject.dataset, tableObject.sourceDbSchemaName, tableObject.table,
                             tableObject.targetSchema, tableObject.stagingSchema, tableObject.batchId,
                             tableObject.numEvents, blob, tableObject.snapshotOnly, tableObject.jsonFormat);
      }));
    }

    IOException error = null;
    for (Future<TableBlob> writeFuture : writeFutures) {
      try {
        TableBlob writtenObject = getWriteFuture(writeFuture);
        Collection<TableBlob> collection = writtenObject.isSnapshotOnly() ? result.get(BlobType.SNAPSHOT) :
          result.get(BlobType.STREAMING);
        collection.add(writtenObject);
      } catch (InterruptedException e) {
        objects.clear();
        throw e;
      } catch (IOException e) {
        if (error == null) {
          error = e;
        } else {
          error.addSuppressed(e);
        }
      }
    }

    objects.clear();
    if (error != null) {
      throw error;
    }
    return result;
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
    private final boolean snapshotOnly;

    private Key(String database, String table, boolean snapshotOnly) {
      this.database = database;
      this.table = table;
      this.snapshotOnly = snapshotOnly;
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
      return snapshotOnly == key.snapshotOnly &&
        Objects.equals(database, key.database) &&
        Objects.equals(table, key.table);
    }

    @Override
    public int hashCode() {
      return Objects.hash(database, table, snapshotOnly);
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
    private final String sourceDbSchemaName;
    private final BlobId blobId;
    private final boolean snapshotOnly;
    private final boolean jsonFormat;
    private int numEvents;
    private Schema stagingSchema;
    private Schema targetSchema;
    private EventWriter eventWriter;

    private TableObject(String dataset, @Nullable String sourceDbSchemaName, String table, boolean snapshotOnly,
                        boolean jsonFormat) {
      this.dataset = dataset;
      this.sourceDbSchemaName = sourceDbSchemaName;
      this.table = table;
      this.numEvents = 0;
      this.snapshotOnly = snapshotOnly;
      batchId = System.currentTimeMillis();
      String objectName = String.format("%s%s/%s/%d", baseObjectName, dataset, table, batchId);
      blobId = BlobId.of(bucket, objectName);
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Writing staging records to GCS file {}", objectName);
      }
      outputStream = Channels.newOutputStream(storage.writer(blobInfo));
      this.jsonFormat = jsonFormat;
    }

    private void writeEvent(Sequenced<DMLEvent> sequencedEvent) throws IOException {
      DMLEvent event = sequencedEvent.getEvent();
      StructuredRecord row = event.getRow();
      if (eventWriter == null) {
        targetSchema = getTargetSchema(row.getSchema());
        stagingSchema = getStagingSchema(row.getSchema());

        if (jsonFormat) {
          eventWriter = new JsonEventWriter(outputStream);
        } else {
          org.apache.avro.Schema avroSchema = schemaMap.computeIfAbsent(snapshotOnly ? targetSchema : stagingSchema,
                                                                        s -> {
                                                                          org.apache.avro.Schema.Parser parser
                                                                            = new org.apache.avro.Schema.Parser();
                                                                          return parser.parse(s.toString());
                                                                        });
          eventWriter = new AvroEventWriter(avroSchema, outputStream);
        }
      }

      StructuredRecord record = createRecord(sequencedEvent);
      eventWriter.write(record);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Writing record {} to GCS.", GSON.toJson(record));
      }
      numEvents++;
    }

    private void close() throws IOException {
      if (eventWriter != null) {
        try {
          eventWriter.close();
        } finally {
          outputStream.close();
        }
      } else {
        outputStream.close();
      }
    }

    private Schema getTargetSchema(Schema schema) {
      List<Schema.Field> fields = new ArrayList<>(schema.getFields().size() + 4);
      fields.addAll(schema.getFields());
      fields.add(Schema.Field.of(Constants.SEQUENCE_NUM, Schema.of(Schema.Type.LONG)));
      fields.add(Schema.Field.of(Constants.IS_DELETED, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));
      fields.add(Schema.Field.of(Constants.ROW_ID, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
      fields.add(Schema.Field.of(Constants.SOURCE_TIMESTAMP, Schema.nullableOf(Schema.of(Schema.Type.LONG))));
      return Schema.recordOf(schema.getRecordName() + ".target", fields);
    }

    /*
        Staging table schema depends on the type of source used in the pipeline.

        If source used is generating ORDERED events, then staging table contains information about
        the batch id, sequence number, operation type, the previous value, and the new value.
        Previous value is null except for update events.

        For example, suppose the source table has two columns -- id (long) and name (string).
        The staging table would have the following columns:

          _op (string)
          _batch_id (long)
          _sequence_num (long)
          id (long)
          name (string)
          _before_id (long)
          _before_name (string)

        If the source used in the pipeline is generating UN-ORDERED events, then for the same example above,
        staging table would contain following columns:

          _op (string)
          _batch_id (long)
          _sequence_num (long)
          _row_id (string)
          id (long)
          name (string)
     */
    private Schema getStagingSchema(Schema schema) {
      int fieldLength =
        rowIdSupported ? schema.getFields().size() + 4 // source schema fields + _op, _batch_id, _sequence_num, _row_id
          : 2 * schema.getFields().size() + 3; // (source schema + _before_ columns) + _op, _batch_id, _sequence_num

      if (eventOrdering == SourceProperties.Ordering.UN_ORDERED) {
        // add column _source_timestamp
        fieldLength++;
      }
      List<Schema.Field> fields = new ArrayList<>(fieldLength);

      fields.add(Schema.Field.of(Constants.OPERATION, Schema.of(Schema.Type.STRING)));
      fields.add(Schema.Field.of(Constants.BATCH_ID, Schema.of(Schema.Type.LONG)));
      fields.add(Schema.Field.of(Constants.SEQUENCE_NUM, Schema.of(Schema.Type.LONG)));
      if (eventOrdering == SourceProperties.Ordering.UN_ORDERED) {
        fields.add(Schema.Field.of(Constants.SOURCE_TIMESTAMP, Schema.of(Schema.Type.LONG)));
      }

      // add all fields from source schema
      fields.addAll(schema.getFields());

      if (rowIdSupported) {
        // add _row_id field to handle un-ordered events
        fields.add(Schema.Field.of(Constants.ROW_ID, Schema.of(Schema.Type.STRING)));
      } else {
        // add _before_ fields for ORDERED events only
        for (Schema.Field field : schema.getFields()) {
          String beforeName = "_before_" + field.getName();
          Schema beforeSchema = field.getSchema();
          beforeSchema = beforeSchema.isNullable() ? beforeSchema : Schema.nullableOf(beforeSchema);
          fields.add(Schema.Field.of(beforeName, beforeSchema));
        }
      }

      return Schema.recordOf(schema.getRecordName() + ".staging", fields);
    }

    private StructuredRecord createRecord(Sequenced<DMLEvent> sequencedEvent) {
      DMLEvent event = sequencedEvent.getEvent();
      StructuredRecord row = event.getRow();
      StructuredRecord.Builder builder;
      if (snapshotOnly) {
        builder = StructuredRecord.builder(targetSchema);
      } else {
        // _op and _batch_id are only part of the staging schema
        builder = StructuredRecord.builder(stagingSchema);
        builder.set(Constants.OPERATION, event.getOperation().getType().name());
        builder.set(Constants.BATCH_ID, batchId);
      }

      builder.set(Constants.SEQUENCE_NUM, sequencedEvent.getSequenceNumber());
      for (Schema.Field field : row.getSchema().getFields()) {
        builder.set(field.getName(), row.get(field.getName()));
      }

      if (eventOrdering == SourceProperties.Ordering.UN_ORDERED) {
        builder.set(Constants.SOURCE_TIMESTAMP, event.getSourceTimestampMillis());
      }

      if (rowIdSupported) {
        builder.set(Constants.ROW_ID, event.getRowId());
        return builder.build();
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
              event.getOperation().getDatabaseName(), event.getOperation().getTableName()));
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
