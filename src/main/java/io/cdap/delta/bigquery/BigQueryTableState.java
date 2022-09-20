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

import io.cdap.cdap.api.data.schema.Schema;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;


/**
 * Stores Big Query table state during the replication process.
 */
public class BigQueryTableState {

  private final List<String> primaryKeys;

  @Nullable
  private List<Schema.Type> sortKeys;

  public BigQueryTableState(List<String> primaryKeys) {
    this(primaryKeys, null);
  }

  public BigQueryTableState(List<String> primaryKeys, List<Schema.Type> sortKeys) {
    this.primaryKeys = Collections.unmodifiableList(new ArrayList<>(primaryKeys));
    if (sortKeys != null) {
      this.sortKeys = Collections.unmodifiableList(new ArrayList<>(sortKeys));
    }
  }

  public List<String> getPrimaryKeys() {
    return primaryKeys;
  }

  @Nullable
  public List<Schema.Type> getSortKeys() {
    return sortKeys;
  }

  public void setSortKeys(@Nullable List<Schema.Type> sortKeys) {
    this.sortKeys = sortKeys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BigQueryTableState that = (BigQueryTableState) o;
    return Objects.equals(primaryKeys, that.primaryKeys)
            && Objects.equals(sortKeys, that.sortKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(primaryKeys, sortKeys);
  }
}
