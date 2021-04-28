package io.cdap.delta.bigquery.event;

import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DeltaFailureException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EventDispatcher {

    private final Map<DDLOperation.Type, DDLEventHandler> handlers = new HashMap<>();

    public EventDispatcher(
            DropDatabase dropDatabase,
            CreateDatabase createDatabase,
            DropTable dropTable,
            CreateTable createTable,
            AlterTable alterTable,
            TruncateTable truncateTable,
            RenameTable renameTable
    ) {
        handlers.put(DDLOperation.Type.DROP_DATABASE, dropDatabase);
        handlers.put(DDLOperation.Type.CREATE_DATABASE, createDatabase);
        handlers.put(DDLOperation.Type.DROP_TABLE, dropTable);
        handlers.put(DDLOperation.Type.CREATE_TABLE, createTable);
        handlers.put(DDLOperation.Type.ALTER_TABLE, alterTable);
        handlers.put(DDLOperation.Type.TRUNCATE_TABLE, truncateTable);
        handlers.put(DDLOperation.Type.RENAME_TABLE, renameTable);
    }

    public DDLEventHandler handler(DDLEvent event) {
        return handlers.get(event.getOperation().getType());
    }

}
