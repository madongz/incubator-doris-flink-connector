package org.apache.doris.flink.sink.writer;

import java.io.IOException;
import java.io.Serializable;

public interface DorisRecordSerializer<T> extends Serializable {
    byte[] serialize(T record) throws IOException;
}
