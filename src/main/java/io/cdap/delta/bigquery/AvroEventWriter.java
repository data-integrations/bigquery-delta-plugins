/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * EventWriter that writes records in Avro format.
 */
public class AvroEventWriter implements EventWriter {
  private DataFileWriter<StructuredRecord> avroWriter;

  AvroEventWriter(org.apache.avro.Schema avroSchema, OutputStream outputStream) {
    DatumWriter<StructuredRecord> datumWriter = new RecordDatumWriter();
    try {
      avroWriter = new DataFileWriter<>(datumWriter).create(avroSchema, outputStream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create avro event writer.", e);
    }
  }

  @Override
  public void write(StructuredRecord record) throws IOException {
    avroWriter.append(record);
  }

  @Override
  public void close() throws IOException {
    avroWriter.close();
  }
}
