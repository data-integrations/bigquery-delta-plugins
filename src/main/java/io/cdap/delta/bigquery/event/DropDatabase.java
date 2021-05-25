package io.cdap.delta.bigquery.event;

import com.google.cloud.bigquery.DatasetId;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.bigquery.BigQueryEventConsumer;

import java.io.IOException;

public class DropDatabase extends DDLEventHandler {

    public DropDatabase(BigQueryEventConsumer consumer, EventHandlerConfig config) { super(consumer, config); }

    @Override
    public void handleDDL(DDLEvent event, String normalizedDatabaseName, String normalizedTableName, String normalizedStagingTableName) throws IOException, DeltaFailureException, InterruptedException {
        DatasetId datasetId = DatasetId.of(config.project, normalizedDatabaseName);
        config.primaryKeyStore.clear();
        if (config.bigQuery.getDataset(datasetId) != null) {
            if (config.requireManualDrops) {
                String message = String.format("Encountered an event to drop dataset '%s' in project '%s', " +
                                "but the target is configured to require manual drops. " +
                                "Please manually drop the dataset to make progress.",
                        normalizedDatabaseName, config.project);
                LOG.error(message);
                throw new RuntimeException(message);
            }
            config.bigQuery.delete(datasetId);
        }

    }
}
