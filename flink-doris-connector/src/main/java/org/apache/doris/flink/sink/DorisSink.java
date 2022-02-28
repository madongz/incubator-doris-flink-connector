package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.committer.DorisCommitter;
import org.apache.doris.flink.sink.writer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.DorisWriter;
import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.DorisWriterStateSerializer;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class DorisSink<IN> implements Sink<IN, DorisCommittable, DorisWriterState, DorisCommittable> {

    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final DorisExecutionOptions dorisExecutionOptions;
    private final DorisRecordSerializer<IN> serializer;
    public DorisSink(DorisOptions dorisOptions,
                     DorisReadOptions dorisReadOptions,
                     DorisExecutionOptions dorisExecutionOptions,
                     DorisRecordSerializer<IN> serializer) {
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.dorisExecutionOptions = dorisExecutionOptions;
        this.serializer = serializer;
    }
    @Override
    public SinkWriter<IN, DorisCommittable, DorisWriterState> createWriter(InitContext initContext, List<DorisWriterState> state) throws IOException {
        DorisWriter<IN> dorisWriter = new DorisWriter<IN>(initContext, state, serializer, dorisOptions, dorisReadOptions, dorisExecutionOptions);
        dorisWriter.initializeLoad(state);
        return dorisWriter;
    }

    @Override
    public Optional<SimpleVersionedSerializer<DorisWriterState>> getWriterStateSerializer() {
        return Optional.of(new DorisWriterStateSerializer());
    }

    @Override
    public Optional<Committer<DorisCommittable>> createCommitter() throws IOException {
        return Optional.of(new DorisCommitter(dorisOptions, dorisReadOptions, dorisExecutionOptions.getMaxRetries()));
    }

    @Override
    public Optional<GlobalCommitter<DorisCommittable, DorisCommittable>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DorisCommittable>> getCommittableSerializer() {
        return Optional.of(new DorisCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<DorisCommittable>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }
    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }

    public static class Builder<IN> {
        private DorisOptions dorisOptions;
        private DorisReadOptions dorisReadOptions;
        private DorisExecutionOptions dorisExecutionOptions;
        private DorisRecordSerializer<IN> serializer;

        public Builder<IN> setDorisOptions(DorisOptions dorisOptions) {
            this.dorisOptions = dorisOptions;
            return this;
        }

        public Builder<IN> setDorisReadOptions(DorisReadOptions dorisReadOptions) {
            this.dorisReadOptions = dorisReadOptions;
            return this;
        }

        public Builder<IN> setDorisExecutionOptions(DorisExecutionOptions dorisExecutionOptions) {
            this.dorisExecutionOptions = dorisExecutionOptions;
            return this;
        }

        public Builder<IN> setSerializer(DorisRecordSerializer<IN> serializer) {
            this.serializer = serializer;
            return this;
        }

        public DorisSink<IN> build() {
            Preconditions.checkNotNull(dorisOptions);
            Preconditions.checkNotNull(dorisReadOptions);
            Preconditions.checkNotNull(dorisExecutionOptions);
            Preconditions.checkNotNull(serializer);
            EscapeHandler.handleEscape(dorisExecutionOptions.getStreamLoadProp());
            return new DorisSink<>(dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
        }
    }
}
