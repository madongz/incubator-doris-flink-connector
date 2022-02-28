package org.apache.doris.flink.sink.writer;

import java.util.Objects;

public class DorisWriterState {
    String labelPrefix;
    public DorisWriterState(String labelPrefix) {
        this.labelPrefix = labelPrefix;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DorisWriterState that = (DorisWriterState) o;
        return Objects.equals(labelPrefix, that.labelPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labelPrefix);
    }

    @Override
    public String toString() {
        return "DorisWriterState{" +
                "labelPrefix='" + labelPrefix + '\'' +
                '}';
    }
}
