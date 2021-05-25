package io.cdap.delta.bigquery.event;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Bucket;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.bigquery.BigQueryEventConsumer;
import io.cdap.delta.bigquery.BigQueryTarget;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CreateDatabase extends DDLEventHandler {

    public CreateDatabase(BigQueryEventConsumer consumer, EventHandlerConfig config) { super(consumer, config); }

    @Override
    public void handleDDL(DDLEvent event, String normalizedDatabaseName, String normalizedTableName, String normalizedStagingTableName) throws IOException, DeltaFailureException, InterruptedException {
        DatasetId datasetId = DatasetId.of(config.project, normalizedDatabaseName);
        if (config.bigQuery.getDataset(datasetId) == null) {
            DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).setLocation(config.bucket.getLocation()).build();
            try {
                config.bigQuery.create(datasetInfo);
            } catch (BigQueryException e) {
                // It is possible that in multiple worker instances scenario
                // dataset is created by another worker instance after this worker instance
                // determined that dataset does not exists. Ignore error if dataset is created.
                if (e.getCode() != BigQueryTarget.CONFLICT) {
                    throw e;
                }
            }
        }
    }
}
