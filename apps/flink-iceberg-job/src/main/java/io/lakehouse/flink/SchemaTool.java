package io.lakehouse.flink;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.types.Types;

/** Metadata-only schema change: java -cp job.jar io.lakehouse.flink.SchemaTool add-column region string */
public final class SchemaTool {

    private SchemaTool() {}

    public static void main(String[] args) {
        if (args.length != 3 || !"add-column".equals(args[0])) {
            System.err.println("usage: SchemaTool add-column <name> <string|long|double|boolean>");
            System.exit(2);
        }
        JobConfig config = JobConfig.fromEnv(System.getenv());
        Catalog catalog = IcebergTables.openCatalog(config);
        try {
            Table table = IcebergTables.ensureTable(catalog, config);
            if (table.schema().findField(args[1]) != null) {
                System.out.println("column '" + args[1] + "' already exists; nothing to do");
                return;
            }
            table.updateSchema().addColumn(args[1], type(args[2])).commit();
            System.out.println("added column '" + args[1] + "'; table schema is now: " + table.schema().asStruct());
        } finally {
            IcebergTables.close(catalog);
        }
    }

    private static org.apache.iceberg.types.Type type(String name) {
        return switch (name) {
            case "string" -> Types.StringType.get();
            case "long" -> Types.LongType.get();
            case "double" -> Types.DoubleType.get();
            case "boolean" -> Types.BooleanType.get();
            default -> throw new IllegalArgumentException("unsupported type " + name);
        };
    }
}
