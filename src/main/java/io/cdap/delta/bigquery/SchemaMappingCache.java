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
  private final Map<Schema, Schema> schemaMapping = new MapMaker()
    .weakKeys().makeMap();

  public void reset(){
    schemaMapping.clear();
  }

  public void put(Schema key, Schema value){
    schemaMapping.put(key, value);
  }

  public Schema get(Schema key){
    return schemaMapping.get(key);
  }
}
