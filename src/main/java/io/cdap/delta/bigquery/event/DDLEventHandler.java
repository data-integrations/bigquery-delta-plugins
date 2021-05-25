package io.cdap.delta.bigquery.event;

import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.bigquery.BigQueryEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class DDLEventHandler {

    final static protected Logger LOG = LoggerFactory.getLogger(DDLEventHandler.class);

    final protected BigQueryEventConsumer consumer;
    final protected EventHandlerConfig config;

    public DDLEventHandler(BigQueryEventConsumer consumer, EventHandlerConfig config) {
        this.consumer = consumer;
        this.config = config;
    }

    public abstract void handleDDL(
            DDLEvent event,
            String normalizedDatabaseName,
            String normalizedTableName,
            String normalizedStagingTableName)
            throws IOException, DeltaFailureException, InterruptedException
    ;

}
