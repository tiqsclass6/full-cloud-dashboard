package io.lakehouse.ingest;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public final class EventValidator {

    static final int MAX_SOURCE_LENGTH = 128;
    static final int MAX_TYPE_LENGTH = 64;
    static final int MAX_ATTRIBUTES = 32;
    static final int MAX_ATTRIBUTE_KEY_LENGTH = 64;
    static final int MAX_ATTRIBUTE_VALUE_LENGTH = 1024;
    static final Duration MAX_AGE = Duration.ofDays(7);
    static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(5);

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9._:-]+");

    private final Clock clock;

    public EventValidator(Clock clock) {
        this.clock = clock;
    }

    /** Returns human-readable problems for one event, or an empty list if it is valid. */
    public List<String> validate(EventRequest event) {
        List<String> errors = new ArrayList<>();
        checkName("source", event.source(), MAX_SOURCE_LENGTH, errors);
        checkName("type", event.type(), MAX_TYPE_LENGTH, errors);
        if (event.region() != null) {
            checkName("region", event.region(), MAX_TYPE_LENGTH, errors);
        }

        // Required: a retry can only be recognised as a copy if the source chose the id before the first send.
        if (event.eventId() == null || event.eventId().isBlank()) {
            errors.add("event_id is required");
        } else {
            try {
                UUID.fromString(event.eventId());
            } catch (IllegalArgumentException e) {
                errors.add("event_id must be a UUID");
            }
        }

        if (event.timestamp() != null) {
            long now = clock.millis();
            if (event.timestamp() < now - MAX_AGE.toMillis()) {
                errors.add("timestamp is more than " + MAX_AGE.toDays() + " days in the past");
            } else if (event.timestamp() > now + MAX_FUTURE_SKEW.toMillis()) {
                errors.add("timestamp is more than " + MAX_FUTURE_SKEW.toMinutes() + " minutes in the future");
            }
        }

        if (event.value() != null && !Double.isFinite(event.value())) {
            errors.add("value must be a finite number");
        }

        Map<String, String> attributes = event.attributes();
        if (attributes != null) {
            if (attributes.size() > MAX_ATTRIBUTES) {
                errors.add("attributes has more than " + MAX_ATTRIBUTES + " entries");
            }
            attributes.forEach((key, value) -> {
                if (key == null || key.isBlank() || key.length() > MAX_ATTRIBUTE_KEY_LENGTH) {
                    errors.add("attribute keys must be 1-" + MAX_ATTRIBUTE_KEY_LENGTH + " characters");
                }
                if (value == null || value.length() > MAX_ATTRIBUTE_VALUE_LENGTH) {
                    errors.add("attribute values must be non-null and at most " + MAX_ATTRIBUTE_VALUE_LENGTH
                            + " characters");
                }
            });
        }
        return errors;
    }

    private static void checkName(String field, String value, int maxLength, List<String> errors) {
        if (value == null || value.isBlank()) {
            errors.add(field + " is required");
        } else if (value.length() > maxLength) {
            errors.add(field + " must be at most " + maxLength + " characters");
        } else if (!NAME.matcher(value).matches()) {
            errors.add(field + " may only contain letters, digits and . _ : -");
        }
    }
}
