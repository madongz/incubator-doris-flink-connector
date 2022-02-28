package org.apache.doris.flink.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

class DorisCommittableSerializer implements SimpleVersionedSerializer<DorisCommittable> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DorisCommittable dorisCommittable) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(dorisCommittable.getHostPort());
            out.writeUTF(dorisCommittable.getDb());
            out.writeLong(dorisCommittable.getTxnID());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DorisCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            final String hostPort = in.readUTF();
            final String db = in.readUTF();
            final long txnId = in.readLong();
            return new DorisCommittable(hostPort, db, txnId);
        }
    }
}
