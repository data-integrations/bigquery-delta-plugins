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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static com.google.common.collect.Iterators.getOnlyElement;

public class BigQueryAssessorTest {
  @Test
  public void testAssessTable_duplicatedTableName() {
    BigQueryAssessor assessor = new BigQueryAssessor("staging_prefix");
    String dbName = "testDB";
    String tableName = "tableName";
    String schemaName1 = "schemaName1";
    String schemaName2 = "schemaName2";
    String fieldName1 = "field1";
    String fieldName2 = "field2";
    Schema tableSchema = Schema.recordOf(tableName,
      new Schema.Field[]{Schema.Field.of(fieldName1, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(fieldName2, Schema.of(Schema.Type.INT))});
    StandardizedTableDetail tableDetail1 =
      new StandardizedTableDetail(dbName, schemaName1, tableName, Arrays.asList(fieldName1), tableSchema);

    StandardizedTableDetail tableDetail2 =
      new StandardizedTableDetail(dbName, schemaName2, tableName, Arrays.asList(fieldName1), tableSchema);

    TableAssessment tableAssessment = assessor.assess(tableDetail1);
    Assert.assertTrue(tableAssessment.getFeatureProblems().isEmpty());
    // assess same table in same schema should be fine;
    tableAssessment = assessor.assess(tableDetail1);
    Assert.assertTrue(tableAssessment.getFeatureProblems().isEmpty());
    tableAssessment = assessor.assess(tableDetail2);
    Assert.assertEquals(1, tableAssessment.getFeatureProblems().size());
    Assert.assertEquals(new Problem("Duplicate Table Name", String.format(
      "Table with name '%s' found in two different " +
        "schemas, '%s' and '%s'. BigQuery target requires table names to be unique across the schemas. Please " +
        "select table from one of the schemas only for replication.", tableName, schemaName1, schemaName2),
        "Please only select one of the tables with same table name to replicate",
        "Not be able to replicate multiple tables with same name to BigQuery"),
      getOnlyElement(tableAssessment.getFeatureProblems().iterator()));
  }

}
