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
