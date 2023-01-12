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

package io.cdap.delta.transformation;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.transformation.api.ColumnInfo;
import io.cdap.transformation.api.ColumnSchema;
import io.cdap.transformation.api.ColumnValue;
import io.cdap.transformation.api.Transformation;
import io.cdap.transformation.api.TransformationContext;
import io.cdap.transformation.api.TransformationDefinitionContext;
import io.cdap.transformation.api.TransformationSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Mock rename transformation
 */
@Plugin(type = Transformation.PLUGIN_TYPE)
@Name("rename")
public class MockRenameTransformation implements Transformation {
  private final MockRenameTransformationConfig config;
  private String directive;
  private String srcColumn;
  private String tgtColumn;

  public MockRenameTransformation(MockRenameTransformationConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformationContext context) throws Exception {
  }

  private void parseDirective() {
    String[] splits = directive.split(" ");
    srcColumn = splits[1];
    tgtColumn = splits[2];
  }

  @Override
  public TransformationSpec define(TransformationDefinitionContext context) {
    this.directive = context.getDirective();
    parseDirective();
    return getSecification();
  }

  @Override
  public TransformationSpec getSecification() {
    return new TransformationSpec() {
      @Override
      public List<ColumnInfo> getInputColumns() {
        return Arrays.asList(new MockColumnInfo(srcColumn));
      }

      @Override
      public List<ColumnInfo> getOutputColumns() {
        return Arrays.asList(new MockColumnInfo(srcColumn));
      }
    };
  }

  @Override
  public Map<String, ColumnValue> transformValue(Map<String, ColumnValue> input) throws Exception {
    Object value = input.remove(srcColumn);
    input.put(tgtColumn, new MockColumnValue(value));
    return input;
  }

  @Override
  public List<ColumnSchema> transformSchema(Map<String, ColumnSchema> input) throws Exception {


    ColumnSchema schema = input.get(srcColumn);
    return Arrays.asList(new MockColumnSchema(Schema.Field.of(tgtColumn, schema.getField().getSchema())));
  }

  @Override
  public String getDirective() {
    return directive;
  }
}
