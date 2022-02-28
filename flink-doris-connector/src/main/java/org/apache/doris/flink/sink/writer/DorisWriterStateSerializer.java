package org.apache.doris.flink.sink.writer;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DorisWriterStateSerializer implements SimpleVersionedSerializer<DorisWriterState> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DorisWriterState dorisWriterState) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(dorisWriterState.getLabelPrefix());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DorisWriterState deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            final String labelPrefix = in.readUTF();
            return new DorisWriterState(labelPrefix);
        }
    }
}
