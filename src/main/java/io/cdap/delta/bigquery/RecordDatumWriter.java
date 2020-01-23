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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.format.io.StructuredRecordDatumWriter;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

import java.io.IOException;

/**
 * An avro datum writer for CDAP structured records.
 */
public class RecordDatumWriter implements DatumWriter<StructuredRecord> {
  private final io.cdap.cdap.common.io.DatumWriter<StructuredRecord> delegate;

  public RecordDatumWriter() {
    this.delegate = new StructuredRecordDatumWriter();
  }

  @Override
  public void setSchema(Schema schema) {
    // no-op
  }

  @Override
  public void write(StructuredRecord structuredRecord, Encoder encoder) throws IOException {
    delegate.encode(structuredRecord, EncoderBridge.wrap(encoder));
  }
}
