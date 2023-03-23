/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.common.collect.MapMaker;
import io.cdap.cdap.api.data.schema.Schema;

import java.util.Map;

public class SchemaMappingCache {
  private final Map<Schema, SchemaMapping> cache = new MapMaker()
    .weakKeys().makeMap();


  static class SchemaMapping {
    private Schema mappedSchema;
    private Map<String, String> fieldNameMapping;

    public SchemaMapping(Schema mappedSchema, Map<String, String> fieldNameMapping) {
      this.mappedSchema = mappedSchema;
      this.fieldNameMapping = fieldNameMapping;
    }

    public Schema getMappedSchema() {
      return mappedSchema;
    }

    public Map<String, String> getFieldNameMapping() {
      return fieldNameMapping;
    }
  }

  public void reset() {
    cache.clear();
  }

  public void put(Schema key, SchemaMapping mappedSchema) {
    cache.put(key, mappedSchema);
  }

  public SchemaMapping get(Schema key) {
    return cache.get(key);
  }
}
