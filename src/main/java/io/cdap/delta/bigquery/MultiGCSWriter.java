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
import io.cdap.delta.api.Sequenced;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

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

  public MultiGCSWriter(Storage storage, String bucket, String baseObjectName) {
    this.storage = storage;
    this.bucket = bucket;
    this.baseObjectName = baseObjectName;
    this.objects = new HashMap<>();
    this.schemaMap = new HashMap<>();
  }

  public synchronized void write(Sequenced<DMLEvent> sequencedEvent) {
    DMLEvent event = sequencedEvent.getEvent();
    Key key = new Key(event.getDatabase(), event.getTable());
    TableObject tableObject = objects.computeIfAbsent(key, t -> new TableObject());
    try {
      tableObject.writeEvent(sequencedEvent);
    } catch (IOException e) {
      // this should never happen, as it's writing to an in memory byte[]
      throw new IllegalStateException(String.format("Unable to write event %s to bytes.", event), e);
    }
  }

  public synchronized Collection<TableBlob> flush() {
    // TODO: write in parallel
    List<TableBlob> writtenObjects = new ArrayList<>(objects.size());
    for (Map.Entry<Key, TableObject> entry : objects.entrySet()) {
      TableObject tableObject = entry.getValue();
      String dataset = entry.getKey().database;
      String table = entry.getKey().table;
      String objectName = String.format("%s%s/%s/%s", baseObjectName, dataset, table, tableObject.batchId);
      BlobId blobId = BlobId.of(bucket, objectName);
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      byte[] content = tableObject.close();
      // TODO: retry on failure
      Blob blob = storage.create(blobInfo, content, Storage.BlobTargetOption.doesNotExist());
      writtenObjects.add(new TableBlob(dataset, table, tableObject.targetSchema, tableObject.stagingSchema,
                                       tableObject.batchId, blob));
    }
    objects.clear();
    return writtenObjects;
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
    private final ByteArrayOutputStream outputStream;
    private final String batchId;
    private Schema stagingSchema;
    private Schema targetSchema;
    private DataFileWriter<StructuredRecord> avroWriter;

    private TableObject() {
      outputStream = new ByteArrayOutputStream();
      batchId = String.format("%d-%s", System.currentTimeMillis(), UUID.randomUUID());
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
    }

    private byte[] close() {
      if (avroWriter != null) {
        try {
          avroWriter.close();
        } catch (IOException e) {
          // should never happen since it's an in-memory byte stream
        }
        return outputStream.toByteArray();
      }
      return new byte[0];
    }

    private Schema getTargetSchema(Schema schema) {
      List<Schema.Field> fields = new ArrayList<>(schema.getFields().size() + 1);
      fields.addAll(schema.getFields());
      fields.add(Schema.Field.of("_sequence_num", Schema.of(Schema.Type.LONG)));
      return Schema.recordOf(schema.getRecordName() + ".target", fields);
    }

    // the original schema plus _op, _batch_id, _sequence_num
    private Schema getStagingSchema(Schema schema) {
      List<Schema.Field> fields = new ArrayList<>(schema.getFields().size() + 3);
      fields.addAll(schema.getFields());
      fields.add(Schema.Field.of("_op", Schema.of(Schema.Type.STRING)));
      fields.add(Schema.Field.of("_batch_id", Schema.of(Schema.Type.STRING)));
      fields.add(Schema.Field.of("_sequence_num", Schema.of(Schema.Type.LONG)));
      return Schema.recordOf(schema.getRecordName() + ".staging", fields);
    }

    private StructuredRecord createStagingRecord(Sequenced<DMLEvent> sequencedEvent) {
      DMLEvent event = sequencedEvent.getEvent();
      StructuredRecord row = event.getRow();
      StructuredRecord.Builder builder = StructuredRecord.builder(stagingSchema);
      for (Schema.Field field : row.getSchema().getFields()) {
        builder.set(field.getName(), row.get(field.getName()));
      }
      builder.set("_op", event.getOperation().name());
      builder.set("_batch_id", batchId);
      builder.set("_sequence_num", sequencedEvent.getSequenceNumber());
      return builder.build();
    }
  }
}
