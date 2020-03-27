/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.cloud.bigquery.StandardSQLTypeName;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Assesses table information.
 */
public class BigQueryAssessor implements TableAssessor<StandardizedTableDetail> {
  private static final int MAX_LENGTH = 1024;
  // according to big query dataset and table naming convention, valid name should only contain letters (upper or
  // lower case), numbers, and underscores
  private static final String VALID_NAME_REGEX = "[\\w]+";
  private final String stagingTablePrefix;

  public BigQueryAssessor(String stagingTablePrefix) {
    this.stagingTablePrefix = stagingTablePrefix;
  }

  @Override
  public TableAssessment assess(StandardizedTableDetail tableDetail) {
    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    for (Schema.Field field : tableDetail.getSchema().getFields()) {
      try {
        String bqType = toBigQueryType(field);
        columnAssessments.add(ColumnAssessment.builder(field.getName(), bqType)
                                .setSourceColumn(field.getName())
                                .build());
      } catch (IllegalArgumentException e) {
        columnAssessments.add(ColumnAssessment.builder(field.getName(), "N/A")
                                .setSourceColumn(field.getName())
                                .setSupport(ColumnSupport.NO)
                                .setSuggestion(new ColumnSuggestion(e.getMessage(), Collections.emptyList()))
                                .build());
      }
    }
    List<Problem> problems = new ArrayList<>();
    String dbName = tableDetail.getDatabase();
    String tableName = tableDetail.getTable();
    if (tableDetail.getPrimaryKey().isEmpty()) {
      problems.add(new Problem("Missing Primary Key",
                               String.format("Table '%s' in database '%s' must have a primary key in order to be " +
                                               "replicated", dbName, tableName),
                               "Please alter the table to use a primary key, or select a different table",
                               "Not able to replicate this table in BigQuery side"));
    }

    if (dbName.length() > MAX_LENGTH) {
      problems.add(new Problem("Dataset Name Exceeding Max Length",
                               String.format("Dataset name '%s' exceeds the max length %d defined in BigQuery",
                                             dbName, MAX_LENGTH),
                               "It's better to alter the database to make the name shorter",
                               "There could be issues if multiple datasets would normalize to the same thing " +
                                 "which will cause conflicts"));
    }
    if (!dbName.matches(VALID_NAME_REGEX)) {
      problems.add(new Problem("Dataase Name Not Valid",
                               String.format("Dataset name '%s' is not valid, BigQuery can only contain letters " +
                                               "(upper or lower case), numbers and underscores", dbName),
                               "It's better to alter the database to make the name qualified and unique " +
                                 "across the project",
                               "There could be issues if multiple datasets would normalize to the same thing " +
                                 "which will cause conflicts"));
    }
    if (stagingTablePrefix.length() + tableName.length() > MAX_LENGTH) {
      problems.add(new Problem("Table Name Exceeding Max Length After Adding Staging Prefix",
                               String.format("After adding the staging prefix, the length of full staging table name " +
                                               "'%s' exceeds the max length %d defined in BigQuery",
                                             stagingTablePrefix + tableName, MAX_LENGTH),
                               "It's better to alter the table to make the name shorter",
                               "There could be issues if multiple tables would normalize to the same thing " +
                                 "which will cause conflicts"));
    }
    if (!stagingTablePrefix.matches(VALID_NAME_REGEX)) {
      problems.add(new Problem("Staging Table Prefix Not Valid",
                               String.format("Staging table prefix '%s' is not valid, BigQuery can only contain " +
                                               "letters (upper or lower case), numbers and underscores",
                                             stagingTablePrefix),
                               "It's better to update the prefix name to make it qualified", ""));
    }
    if (!tableName.matches(VALID_NAME_REGEX)) {
      problems.add(new Problem("Table Name Not Valid",
                               String.format("Table name '%s' is not valid, BigQuery can only contain letters " +
                                               "(upper or lower case), numbers and underscores", tableName),
                               "It's better to alter the table name to make it qualified",
                               "There could be issues if multiple tables would normalize to the same thing " +
                                 "which will cause conflicts"));
    }

    return new TableAssessment(columnAssessments, problems);
  }

  private String toBigQueryType(Schema.Field field) {
    Schema schema = field.getSchema();
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DECIMAL:
          return StandardSQLTypeName.NUMERIC.name();
        case DATE:
          return StandardSQLTypeName.DATE.name();
        case TIME_MICROS:
        case TIME_MILLIS:
          return StandardSQLTypeName.TIME.name();
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
          return StandardSQLTypeName.TIMESTAMP.name();
      }
      throw new IllegalArgumentException(String.format("Column '%s' is of unsupported type '%s'",
                                                       field.getName(), schema.getLogicalType().getToken()));
    }

    switch (schema.getType()) {
      case BOOLEAN:
        return StandardSQLTypeName.BOOL.name();
      case FLOAT:
      case DOUBLE:
        return StandardSQLTypeName.FLOAT64.name();
      case STRING:
      case ENUM:
        return StandardSQLTypeName.STRING.name();
      case INT:
      case LONG:
        return StandardSQLTypeName.INT64.name();
      case ARRAY:
        return StandardSQLTypeName.ARRAY.name();
      case BYTES:
        return StandardSQLTypeName.BYTES.name();
      case RECORD:
        return StandardSQLTypeName.STRUCT.name();
    }

    throw new IllegalArgumentException(String.format("Column '%s' is of unsupported type '%s'",
                                                     field.getName(), schema.getType().name().toLowerCase()));
  }
}
