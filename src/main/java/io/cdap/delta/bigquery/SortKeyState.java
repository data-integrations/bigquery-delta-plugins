/*
 *
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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.List;

/**
 * Stores the list of sort key types and if sort key column has been added to the target table
 */
public class SortKeyState {
    private List<Schema.Type> sortKeys;
    private boolean isAddedToTarget;

    public SortKeyState(List<Schema.Type> sortKeys) {
        this.sortKeys = sortKeys;
    }

    public SortKeyState(List<Schema.Type> sortKeys, boolean isAddedToTarget) {
        this.sortKeys = sortKeys;
        this.isAddedToTarget = isAddedToTarget;
    }

    public List<Schema.Type> getSortKeys() {
        return sortKeys;
    }

    public void setSortKeys(List<Schema.Type> sortKeys) {
        this.sortKeys = sortKeys;
    }

    public boolean isAddedToTarget() {
        return isAddedToTarget;
    }

    public void setAddedToTarget(boolean addedToTarget) {
        isAddedToTarget = addedToTarget;
    }
}
