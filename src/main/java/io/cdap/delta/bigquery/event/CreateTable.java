package io.cdap.delta.bigquery.event;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Bucket;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.bigquery.BigQueryEventConsumer;
import io.cdap.delta.bigquery.Schemas;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CreateTable extends DDLEventHandler {

    public CreateTable(BigQueryEventConsumer consumer, EventHandlerConfig config) { super(consumer, config); }

    @Override
    public void handleDDL(DDLEvent event, String normalizedDatabaseName, String normalizedTableName, String normalizedStagingTableName) throws IOException, DeltaFailureException, InterruptedException {
        TableId tableId = TableId.of(config.project, normalizedDatabaseName, normalizedTableName);
        Table table = config.bigQuery.getTable(tableId);
        // SNAPSHOT data is directly loaded in the target table. Check if any such direct load was in progress
        // for the current table when target received CREATE_TABLE ddl. This indicates that the snapshot was abandoned
        // because of some failure scenario. Delete the existing table if any.
        byte[] state = config.context.getState(String.format(BigQueryEventConsumer.DIRECT_LOADING_IN_PROGRESS_PREFIX + "%s-%s",
                normalizedDatabaseName, normalizedTableName));
        if (table != null && state != null && Bytes.toBoolean(state)) {
            config.bigQuery.delete(tableId);
        }
        List<String> primaryKeys = event.getPrimaryKey();
        consumer.updatePrimaryKeys(tableId, primaryKeys);
        // TODO: check schema of table if it exists already
        if (table == null) {
            List<String> clusteringSupportedKeys = BigQueryEventConsumer.getClusteringSupportedKeys(primaryKeys, event.getSchema());
            Clustering clustering = config.maxClusteringColumns <= 0 || clusteringSupportedKeys.isEmpty() ? null :
                    Clustering.newBuilder()
                            .setFields(clusteringSupportedKeys.subList(0, Math.min(config.maxClusteringColumns,
                                    clusteringSupportedKeys.size())))
                            .build();
            TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                    .setSchema(Schemas.convert(BigQueryEventConsumer.addSupplementaryColumnsToTargetSchema(event.getSchema())))
                    .setClustering(clustering)
                    .build();

            TableInfo.Builder builder = TableInfo.newBuilder(tableId, tableDefinition);
            if (config.encryptionConfig != null) {
                builder.setEncryptionConfiguration(config.encryptionConfig);
            }
            TableInfo tableInfo = builder.build();
            config.bigQuery.create(tableInfo);
        }
    }
}
