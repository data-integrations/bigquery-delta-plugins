package io.cdap.delta.bigquery.event;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Bucket;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.bigquery.BigQueryEventConsumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RenameTable extends DDLEventHandler {

    public RenameTable(BigQueryEventConsumer consumer, EventHandlerConfig config) { super(consumer, config); }

    @Override
    public void handleDDL(DDLEvent event, String normalizedDatabaseName, String normalizedTableName, String normalizedStagingTableName) throws IOException, DeltaFailureException, InterruptedException {
        // TODO: flush changes, execute a copy job, delete previous table, drop old staging table, remove old entry
        //  in primaryKeyStore, put new entry in primaryKeyStore
        LOG.warn("Rename DDL events are not supported. Ignoring rename event in database {} from table {} to table {}.",
                event.getOperation().getDatabaseName(), event.getOperation().getPrevTableName(),
                event.getOperation().getTableName());

    }
}
