package io.cdap.delta.bigquery.event;

import com.google.cloud.bigquery.*;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.bigquery.BigQueryEventConsumer;
import io.cdap.delta.bigquery.Schemas;

import java.io.IOException;
import java.util.List;

public class TruncateTable extends DDLEventHandler {

    public TruncateTable(BigQueryEventConsumer consumer, EventHandlerConfig config) { super(consumer, config); }

    @Override
    public void handleDDL(DDLEvent event, String normalizedDatabaseName, String normalizedTableName, String normalizedStagingTableName) throws IOException, DeltaFailureException, InterruptedException {
        consumer.flush();
        TableId tableId = TableId.of(config.project, normalizedDatabaseName, normalizedTableName);
        Table table = config.bigQuery.getTable(tableId);
        TableDefinition tableDefinition = null;
        List<String> primaryKeys = null;
        if (table != null) {
            tableDefinition = table.getDefinition();
            config.bigQuery.delete(tableId);
        } else {
            primaryKeys = event.getPrimaryKey();
            Clustering clustering = config.maxClusteringColumns <= 0 ? null :
                    Clustering.newBuilder()
                            .setFields(primaryKeys.subList(0, Math.min(config.maxClusteringColumns, primaryKeys.size())))
                            .build();
            tableDefinition = StandardTableDefinition.newBuilder()
                    .setSchema(Schemas.convert(BigQueryEventConsumer.addSupplementaryColumnsToTargetSchema(event.getSchema())))
                    .setClustering(clustering)
                    .build();
        }

        TableInfo.Builder builder = TableInfo.newBuilder(tableId, tableDefinition);
        if (config.encryptionConfig != null) {
            builder.setEncryptionConfiguration(config.encryptionConfig);
        }
        TableInfo tableInfo = builder.build();
        config.bigQuery.create(tableInfo);

    }
}
