package io.lakehouse.flink;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/** Runs the real operator on an embedded Flink mini-cluster. */
class DeduplicatorTest {

    private static RowData row(String eventId, String payload) {
        return GenericRowData.of(StringData.fromString(eventId), StringData.fromString(payload));
    }

    @Test
    void emitsEachEventIdOnceAndKeepsTheFirstOccurrence() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        InternalTypeInfo<RowData> type = InternalTypeInfo.of(
                RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH)));

        DataStream<RowData> deduped = env
                .fromData(List.of(row("a", "first"), row("b", "x"), row("a", "retry"), row("c", "y"),
                        row("b", "retry"), row("a", "retry-2")), type)
                .keyBy(Deduplicator::keyOf)
                .process(new Deduplicator(Duration.ofHours(1)));

        List<String> out = new ArrayList<>();
        CloseableIterator<RowData> it = deduped.executeAndCollect();
        try {
            it.forEachRemaining(r -> out.add(r.getString(0) + ":" + r.getString(1)));
        } finally {
            it.close();
        }

        assertThat(out).containsExactlyInAnyOrder("a:first", "b:x", "c:y");
    }
}
