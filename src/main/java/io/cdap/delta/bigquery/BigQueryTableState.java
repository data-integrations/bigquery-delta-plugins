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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Stores Big Query table state during the replication process.
 */
public class BigQueryTableState {

  private final List<String> primaryKeys;

  public BigQueryTableState(List<String> primaryKeys) {
    this.primaryKeys = Collections.unmodifiableList(new ArrayList<>(primaryKeys));
  }

  public List<String> getPrimaryKeys() {
    return primaryKeys;
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
    return Objects.equals(primaryKeys, that.primaryKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(primaryKeys);
  }
}
