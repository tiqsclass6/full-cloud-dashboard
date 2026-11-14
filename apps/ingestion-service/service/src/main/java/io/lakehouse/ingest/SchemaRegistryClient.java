package io.lakehouse.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Minimal Confluent-compatible registry client (Apicurio /apis/ccompat/v7): registration only. */
public final class SchemaRegistryClient {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryClient.class);
    private static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private final String baseUrl;
    private final HttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();

    public SchemaRegistryClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    /** Registers the schema under the subject (idempotent) and returns its registry id. */
    public int register(String subject, String schemaJson) throws IOException, InterruptedException {
        String body = mapper.writeValueAsString(mapper.createObjectNode().put("schema", schemaJson));
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(baseUrl + "/subjects/" + URLEncoder.encode(subject, StandardCharsets.UTF_8)
                                + "/versions"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IOException("schema registry returned HTTP " + response.statusCode() + ": " + response.body());
        }
        JsonNode id = mapper.readTree(response.body()).get("id");
        if (id == null || !id.canConvertToInt()) {
            throw new IOException("schema registry response has no integer id: " + response.body());
        }
        return id.asInt();
    }

    /** Like {@link #register} but retries, because the registry may still be starting. */
    public int registerWithRetry(String subject, String schemaJson, int attempts, Duration backoff)
            throws IOException, InterruptedException {
        IOException last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                return register(subject, schemaJson);
            } catch (IOException e) {
                last = e;
                LOG.warn("Schema registration attempt {}/{} failed: {}", attempt, attempts, e.getMessage());
                if (attempt < attempts) {
                    Thread.sleep(backoff.toMillis());
                }
            }
        }
        throw new IOException("could not register schema after " + attempts + " attempts", last);
    }
}
