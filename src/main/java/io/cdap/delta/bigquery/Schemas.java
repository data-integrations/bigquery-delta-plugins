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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utilities around BigQuery schemas.
 */
public class Schemas {

  // Set of BigQuery types supported for clustering per definition
  // https://cloud.google.com/bigquery/docs/creating-clustered-tables#limitations
  private static final Set<StandardSQLTypeName> CLUSTERING_SUPPORTED_TYPES
    = new HashSet<>(Arrays.asList(StandardSQLTypeName.DATE, StandardSQLTypeName.BOOL, StandardSQLTypeName.GEOGRAPHY,
                                  StandardSQLTypeName.INT64, StandardSQLTypeName.NUMERIC, StandardSQLTypeName.STRING,
                                  StandardSQLTypeName.TIMESTAMP, StandardSQLTypeName.DATETIME));

  private Schemas() {
    // no-op
  }


  public static com.google.cloud.bigquery.Schema convert(Schema schema) {
    return com.google.cloud.bigquery.Schema.of(convertFields(schema.getFields()));
  }

  private static List<Field> convertFields(List<Schema.Field> fields) {
    List<Field> output = new ArrayList<>();
    for (Schema.Field field : fields) {
      output.add(convertToBigQueryField(field));
    }
    return output;
  }

  @Nullable
  private static StandardSQLTypeName convertType(Schema.Type type) {
    switch (type) {
      case INT:
      case LONG:
        return StandardSQLTypeName.INT64;
      case FLOAT:
      case DOUBLE:
        return StandardSQLTypeName.FLOAT64;
      case STRING:
      case ENUM:
        return StandardSQLTypeName.STRING;
      case BOOLEAN:
        return StandardSQLTypeName.BOOL;
      case BYTES:
        return StandardSQLTypeName.BYTES;
    }
    return null;
  }

  @Nullable
  private static StandardSQLTypeName convertLogicalType(Schema fieldSchema) {
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType == null) {
      return null;
    }
    switch (logicalType) {
      case TIME_MICROS:
      case TIME_MILLIS:
        return StandardSQLTypeName.TIME;
      case TIMESTAMP_MILLIS:
      case TIMESTAMP_MICROS:
        return StandardSQLTypeName.TIMESTAMP;
      case DATE:
        return StandardSQLTypeName.DATE;
      case DECIMAL:
        int precision = fieldSchema.getPrecision();
        int scale = fieldSchema.getScale();
        if (precision <= 38 && scale <= 9) {
          return StandardSQLTypeName.NUMERIC;
        }
        return StandardSQLTypeName.BIGNUMERIC;
      case DATETIME:
        return StandardSQLTypeName.DATETIME;
    }
    return null;
  }

  /**
   * Check if the BigQuery data type associated with the {@link Schema.Field} can be added
   * as a clustering column while creating BigQuery table.
   */
  public static boolean isClusteringSupported(Schema.Field field) {
    Field bigQueryField = convertToBigQueryField(field);
    return CLUSTERING_SUPPORTED_TYPES.contains(bigQueryField.getType().getStandardType());
  }

  private static Field convertToBigQueryField(Schema.Field field) {
    String name = field.getName();
    boolean isNullable = field.getSchema().isNullable();
    Schema fieldSchema = field.getSchema();
    fieldSchema = isNullable ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    Field.Mode fieldMode = isNullable ? Field.Mode.NULLABLE : Field.Mode.REQUIRED;
    if (logicalType != null) {
      StandardSQLTypeName bqType = convertLogicalType(fieldSchema);
      // TODO: figure out what the correct behavior should be
      if (bqType == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of type '%s', which is not supported in BigQuery.",
                        name, logicalType.getToken()));
      }
      return Field.newBuilder(name, bqType).setMode(fieldMode).build();
    }

    Field output;
    Schema.Type type = isNullable ? field.getSchema().getNonNullable().getType() : field.getSchema().getType();
    if (type == Schema.Type.ARRAY) {
      Schema componentSchema = fieldSchema.getComponentSchema();
      componentSchema = componentSchema.isNullable() ? componentSchema.getNonNullable() : componentSchema;
      StandardSQLTypeName bqType = convertType(componentSchema.getType());
      if (bqType == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is an array of '%s', which is not supported in BigQuery.",
                        name, logicalType.getToken()));
      }
      output = Field.newBuilder(name, bqType).setMode(Field.Mode.REPEATED).build();
    } else if (type == Schema.Type.RECORD) {
      List<Field> subFields = convertFields(fieldSchema.getFields());
      output = Field.newBuilder(name, StandardSQLTypeName.STRUCT, FieldList.of(subFields)).build();
    } else {
      StandardSQLTypeName bqType = convertType(type);
      if (bqType == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' is of type '%s', which is not supported in BigQuery.",
                        name, type.name().toLowerCase()));
      }
      output = Field.newBuilder(name, bqType).setMode(fieldMode).build();
    }
    return output;
  }
}
