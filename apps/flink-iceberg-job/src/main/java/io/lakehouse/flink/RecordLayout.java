package io.lakehouse.flink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Types;

/**
 * Where the fields sit in the rows the source emits: the events table's columns, then {@code error} and
 * {@code payload}, which are set only for a record that could not be decoded (its table columns stay null).
 */
public final class RecordLayout implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType tableType;
    private final int partition;
    private final int offset;
    private final int timestamp;
    private final int eventId;
    private final int source;

    private RecordLayout(Schema table) {
        this.tableType = FlinkSchemaUtil.convert(table);
        this.partition = position(table, IcebergTables.KAFKA_PARTITION);
        this.offset = position(table, IcebergTables.KAFKA_OFFSET);
        this.timestamp = position(table, IcebergTables.KAFKA_TIMESTAMP);
        this.eventId = position(table, "event_id");
        this.source = position(table, "source");
    }

    /** Requires the lineage columns (see {@link IcebergTables#ensureLineageColumns}). */
    public static RecordLayout of(Schema table) {
        return new RecordLayout(table);
    }

    private static int position(Schema schema, String name) {
        List<Types.NestedField> columns = schema.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("events table has no column " + name);
    }

    public int columns() {
        return tableType.getFieldCount();
    }

    public int arity() {
        return columns() + 2;
    }

    public int error() {
        return columns();
    }

    public int payload() {
        return columns() + 1;
    }

    public int partition() {
        return partition;
    }

    public int offset() {
        return offset;
    }

    public int timestamp() {
        return timestamp;
    }

    public int eventId() {
        return eventId;
    }

    public int source() {
        return source;
    }

    public RowType tableType() {
        return tableType;
    }

    public InternalTypeInfo<RowData> tableTypeInfo() {
        return InternalTypeInfo.of(tableType);
    }

    /** The source's rows: every column nullable, because an undecodable record has only its Kafka fields. */
    public InternalTypeInfo<RowData> recordTypeInfo() {
        List<LogicalType> types = new ArrayList<>();
        List<String> names = new ArrayList<>(tableType.getFieldNames());
        for (LogicalType type : tableType.getChildren()) {
            types.add(type.copy(true));
        }
        types.add(new VarCharType(true, VarCharType.MAX_LENGTH));
        types.add(new VarBinaryType(true, VarBinaryType.MAX_LENGTH));
        names.add("_error");
        names.add("_payload");
        return InternalTypeInfo.of(RowType.of(types.toArray(LogicalType[]::new), names.toArray(String[]::new)));
    }
}
