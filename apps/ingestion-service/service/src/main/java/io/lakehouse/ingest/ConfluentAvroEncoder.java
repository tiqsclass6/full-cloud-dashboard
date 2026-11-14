package io.lakehouse.ingest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

/** Confluent wire format: magic byte 0, 4-byte schema id, Avro binary. */
public final class ConfluentAvroEncoder {

    static final byte MAGIC_BYTE = 0;
    static final int HEADER_SIZE = 5;

    private final Schema schema;
    private final int schemaId;

    public ConfluentAvroEncoder(Schema schema, int schemaId) {
        this.schema = schema;
        this.schemaId = schemaId;
    }

    public byte[] encode(GenericRecord record) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(256);
        out.write(MAGIC_BYTE);
        out.write(schemaId >>> 24);
        out.write(schemaId >>> 16);
        out.write(schemaId >>> 8);
        out.write(schemaId);
        try {
            // The buffered encoder allocated a 2 KiB buffer per call (~16% of all allocation in JFR).
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            // A writer per call: cheap, and avoids sharing mutable state across virtual threads.
            new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }
}
