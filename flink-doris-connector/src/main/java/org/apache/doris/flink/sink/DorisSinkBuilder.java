package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.writer.DorisRecordSerializer;
import org.apache.flink.util.Preconditions;

public class DorisSinkBuilder<IN> {
    private DorisOptions dorisOptions;
    private DorisReadOptions dorisReadOptions;
    private DorisExecutionOptions dorisExecutionOptions;
    private DorisRecordSerializer<IN> serializer;

    public DorisSinkBuilder<IN> setDorisOptions(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        return this;
    }

    public DorisSinkBuilder<IN> setDorisReadOptions(DorisReadOptions dorisReadOptions) {
        this.dorisReadOptions = dorisReadOptions;
        return this;
    }

    public DorisSinkBuilder<IN> setDorisExecutionOptions(DorisExecutionOptions dorisExecutionOptions) {
        this.dorisExecutionOptions = dorisExecutionOptions;
        return this;
    }

    public DorisSinkBuilder<IN> setSerializer(DorisRecordSerializer<IN> serializer) {
        this.serializer = serializer;
        return this;
    }

    public DorisSink<IN> build() {
        Preconditions.checkNotNull(dorisOptions);
        // TODO: remove read optional.
        Preconditions.checkNotNull(dorisReadOptions);
        Preconditions.checkNotNull(dorisExecutionOptions);
        Preconditions.checkNotNull(serializer);
        return new DorisSink<IN>(dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
    }
}
