package io.cdap.delta.bigquery.event;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Bucket;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.bigquery.BigQueryEventConsumer;
import io.cdap.delta.bigquery.BigQueryTableState;
import io.cdap.delta.bigquery.Schemas;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AlterTable extends DDLEventHandler {

    public AlterTable(BigQueryEventConsumer consumer, EventHandlerConfig config) { super(consumer, config); }

    @Override
    public void handleDDL(DDLEvent event, String normalizedDatabaseName, String normalizedTableName, String normalizedStagingTableName) throws IOException, DeltaFailureException, InterruptedException {

        // need to flush any changes before altering the table to ensure all changes before the schema change
        // are in the table when it is altered.
        consumer.flush();
        // after a flush, the staging table will be gone, so no need to alter it.
        TableId tableId = TableId.of(config.project, normalizedDatabaseName, normalizedTableName);
        Table table = config.bigQuery.getTable(tableId);
        List<String> primaryKeys = event.getPrimaryKey();
        Clustering clustering = config.maxClusteringColumns <= 0 ? null :
                Clustering.newBuilder()
                        .setFields(primaryKeys.subList(0, Math.min(config.maxClusteringColumns, primaryKeys.size())))
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
        if (table == null) {
            config.bigQuery.create(tableInfo);
        } else {
            config.bigQuery.update(tableInfo);
        }

        consumer.updatePrimaryKeys(tableId, primaryKeys);
    }


}
