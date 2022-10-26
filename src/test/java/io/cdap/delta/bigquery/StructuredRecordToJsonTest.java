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

import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class StructuredRecordToJsonTest {

  private static final String UPDATED_COLUMN = "updated";
  public static final Schema DATETIME_SCHEMA = Schema.of(Schema.LogicalType.DATETIME);

  @Mock
  private JsonWriter jsonWriter;

  @Test
  public void testWriteDateTimeNanoPrecision() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-04-19 14:31:12.123456789", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-04-19 14:31:12.123456");
  }

  @Test
  public void testWriteDateTimeNanoPrecisionAlternateFormat() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-04-19T14:31:12.123456789", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-04-19T14:31:12.123456");
  }

  @Test
  public void testWriteDateTimeHundredNanoPrecision() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-04-19 14:31:12.1234567", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-04-19 14:31:12.123456");
  }

  @Test
  public void testWriteDateTimeMicrosecondPrecision() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-04-19 14:31:12.123456", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-04-19 14:31:12.123456");
  }

  @Test
  public void testWriteDateTimeMillisPrecision() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-04-19 14:31:12.123", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-04-19 14:31:12.123");
  }

  @Test
  public void testWriteDateTimeSecondPrecision() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-04-19 14:31:12", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-04-19 14:31:12");
  }

  @Test
  public void testWriteDateTimeSecondPrecisionSingleDigit() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-04-19 4:1:2", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-04-19 4:1:2");
  }

  @Test
  public void testWriteDateTimeWithoutTime() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-04-19", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-04-19");
  }

  @Test
  public void testWriteDateTimeSingleDigitDate() throws IOException {
    StructuredRecordToJson.write(jsonWriter, UPDATED_COLUMN, "2022-4-1", DATETIME_SCHEMA);
    Mockito.verify(jsonWriter, Mockito.times(1)).value("2022-4-1");
  }
}
