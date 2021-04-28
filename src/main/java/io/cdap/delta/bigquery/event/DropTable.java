package io.cdap.delta.bigquery.event;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Bucket;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.bigquery.BigQueryEventConsumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DropTable extends DDLEventHandler {

    public DropTable(BigQueryEventConsumer consumer, EventHandlerConfig config) { super(consumer, config); }

    @Override
    public void handleDDL(DDLEvent event, String normalizedDatabaseName, String normalizedTableName, String normalizedStagingTableName) throws IOException, DeltaFailureException, InterruptedException {
        // need to flush changes before dropping the table, otherwise the next flush will write data that
        // shouldn't exist
        consumer.flush();
        TableId tableId = TableId.of(config.project, normalizedDatabaseName, normalizedTableName);
        config.primaryKeyStore.remove(tableId);
        Table table = config.bigQuery.getTable(tableId);
        if (table != null) {
            if (config.requireManualDrops) {
                String message = String.format("Encountered an event to drop table '%s' in dataset '%s' in project '%s', " +
                                "but the target is configured to require manual drops. " +
                                "Please manually drop the table to make progress.",
                        normalizedTableName, normalizedDatabaseName, config.project);
                LOG.error(message);
                throw new RuntimeException(message);
            }
            config.bigQuery.delete(tableId);
        }
        TableId stagingTableId = TableId.of(config.project, normalizedDatabaseName, normalizedStagingTableName);
        Table stagingTable = config.bigQuery.getTable(stagingTableId);
        if (stagingTable != null) {
            config.bigQuery.delete(stagingTableId);
        }

    }
}
