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

import com.google.gson.internal.bind.JsonTreeWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * EventWriter that writes records in JSON format.
 */
public class JsonEventWriter implements EventWriter {
  private final BufferedWriter jsonWriter;

  JsonEventWriter(OutputStream outputStream) {
    this.jsonWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
  }

  @Override
  public void write(StructuredRecord record) throws IOException {
    try (JsonTreeWriter writer = new JsonTreeWriter()) {
      writer.beginObject();
      for (Schema.Field recordField : Objects.requireNonNull(record.getSchema().getFields())) {
        StructuredRecordToJson.write(writer, recordField.getName(), record.get(recordField.getName()),
                                     recordField.getSchema());
      }
      writer.endObject();
      jsonWriter.write(writer.get().getAsJsonObject().toString());
      jsonWriter.newLine();
    }
  }

  @Override
  public void close() throws IOException {
    jsonWriter.close();
  }
}
