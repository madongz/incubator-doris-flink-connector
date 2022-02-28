package org.apache.doris.flink.sink;

import java.util.Objects;

public class DorisCommittable {
    private final String hostPort;
    private final String db;
    private final long txnID;

    public DorisCommittable(String hostPort, String db, long txnID) {
        this.hostPort = hostPort;
        this.db = db;
        this.txnID = txnID;
    }

    public String getHostPort() {
        return hostPort;
    }

    public String getDb() {
        return db;
    }

    public long getTxnID() {
        return txnID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DorisCommittable that = (DorisCommittable) o;
        return txnID == that.txnID &&
                Objects.equals(hostPort, that.hostPort) &&
                Objects.equals(db, that.db);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostPort, db, txnID);
    }

    @Override
    public String toString() {
        return "DorisCommittable{" +
                "hostPort='" + hostPort + '\'' +
                ", db='" + db + '\'' +
                ", txnID=" + txnID +
                '}';
    }
}
