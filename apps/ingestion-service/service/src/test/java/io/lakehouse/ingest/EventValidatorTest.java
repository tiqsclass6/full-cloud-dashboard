package io.lakehouse.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EventValidatorTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-28T12:00:00Z"), ZoneOffset.UTC);
    private static final String ID = "3f2b8c1e-5c1a-4c55-9a53-1b2f6b1f0c11";
    private final EventValidator validator = new EventValidator(CLOCK);

    private static EventRequest event(String source, String type) {
        return new EventRequest(ID, source, type, null, null, null);
    }

    @Test
    void acceptsMinimalEvent() {
        assertThat(validator.validate(event("sensor-1", "cpu.usage"))).isEmpty();
    }

    @Test
    void acceptsFullEvent() {
        EventRequest full = new EventRequest(
                "3f2b8c1e-5c1a-4c55-9a53-1b2f6b1f0c11", "host:web-01", "cpu.usage",
                CLOCK.millis() - 1000, 0.73, Map.of("region", "us-east-1"));
        assertThat(validator.validate(full)).isEmpty();
    }

    @Test
    void requiresSourceAndType() {
        assertThat(validator.validate(event(null, " "))).containsExactly("source is required", "type is required");
    }

    @Test
    void rejectsIllegalCharactersAndLength() {
        assertThat(validator.validate(event("bad source!", "ok"))).anyMatch(e -> e.contains("may only contain"));
        assertThat(validator.validate(event("x".repeat(129), "ok"))).anyMatch(e -> e.contains("at most 128"));
    }

    @Test
    void validatesOptionalRegion() {
        assertThat(validator.validate(new EventRequest(ID, "s", "t", null, null, null, "us-east-1"))).isEmpty();
        assertThat(validator.validate(new EventRequest(ID, "s", "t", null, null, null, "bad region!")))
                .anyMatch(e -> e.contains("region may only contain"));
    }

    @Test
    void requiresEventId() {
        assertThat(validator.validate(new EventRequest(null, "s", "t", null, null, null)))
                .containsExactly("event_id is required");
        assertThat(validator.validate(new EventRequest(" ", "s", "t", null, null, null)))
                .containsExactly("event_id is required");
    }

    @Test
    void rejectsBadEventId() {
        EventRequest bad = new EventRequest("not-a-uuid", "s", "t", null, null, null);
        assertThat(validator.validate(bad)).containsExactly("event_id must be a UUID");
    }

    @Test
    void rejectsTimestampsOutsideAllowedWindow() {
        long now = CLOCK.millis();
        EventRequest old = new EventRequest(ID, "s", "t", now - 8L * 24 * 3600 * 1000, null, null);
        EventRequest future = new EventRequest(ID, "s", "t", now + 6L * 60 * 1000, null, null);
        assertThat(validator.validate(old)).anyMatch(e -> e.contains("in the past"));
        assertThat(validator.validate(future)).anyMatch(e -> e.contains("in the future"));
    }

    @Test
    void rejectsNonFiniteValues() {
        assertThat(validator.validate(new EventRequest(ID, "s", "t", null, Double.NaN, null)))
                .containsExactly("value must be a finite number");
        assertThat(validator.validate(new EventRequest(ID, "s", "t", null, Double.POSITIVE_INFINITY, null)))
                .containsExactly("value must be a finite number");
    }

    @Test
    void limitsAttributes() {
        Map<String, String> many = new HashMap<>();
        for (int i = 0; i <= EventValidator.MAX_ATTRIBUTES; i++) {
            many.put("k" + i, "v");
        }
        assertThat(validator.validate(new EventRequest(ID, "s", "t", null, null, many)))
                .anyMatch(e -> e.contains("more than 32 entries"));
        assertThat(validator.validate(new EventRequest(ID, "s", "t", null, null, Map.of("k", "v".repeat(1025)))))
                .anyMatch(e -> e.contains("attribute values"));
    }
}
