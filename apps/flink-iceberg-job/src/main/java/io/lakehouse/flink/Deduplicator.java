package io.lakehouse.flink;

import java.time.Duration;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Drops events whose event_id was already seen within the TTL (client retries duplicate the input). With a
 * layout, each dropped copy goes to the {@link SkippedRecords#TAG} side output, so its Kafka offset is accounted for.
 */
public final class Deduplicator extends KeyedProcessFunction<String, RowData, RowData> {

    private static final long serialVersionUID = 2L;

    private final Duration ttl;
    private final RecordLayout layout;
    private transient ValueState<Boolean> seen;
    private transient Counter duplicatesDropped;

    public Deduplicator(Duration ttl) {
        this(ttl, null);
    }

    public Deduplicator(Duration ttl, RecordLayout layout) {
        this.ttl = ttl;
        this.layout = layout;
    }

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-event-id", Types.BOOLEAN);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ttl)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build());
        seen = getRuntimeContext().getState(descriptor);
        // Shows that retried copies really reached Kafka (Prometheus: ..._operator_duplicatesDropped).
        duplicatesDropped = getRuntimeContext().getMetricGroup().counter("duplicatesDropped");
    }

    @Override
    public void processElement(RowData row, Context context, Collector<RowData> out) throws Exception {
        if (seen.value() == null) {
            seen.update(Boolean.TRUE);
            out.collect(row);
        } else {
            duplicatesDropped.inc();
            if (layout != null) {
                context.output(SkippedRecords.TAG, SkippedRecords.of(row, layout, SkippedRecords.DUPLICATE));
            }
        }
    }

    /** Key selector: the event id is column 0 of the table layout. */
    public static String keyOf(RowData row) {
        return row.getString(0).toString();
    }
}
