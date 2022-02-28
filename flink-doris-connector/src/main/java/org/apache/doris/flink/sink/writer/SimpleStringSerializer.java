package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.sink.committer.DorisCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SimpleStringSerializer implements DorisRecordSerializer<String>{
    @Override
    public byte[] serialize(String record) throws IOException {
        return record.getBytes(StandardCharsets.UTF_8);
    }
}
