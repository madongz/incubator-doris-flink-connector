package org.apache.doris.flink.sink.writer;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class RecordBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(RecordBuffer.class);
    private static final int DEFAULT_CAPACITY = 1024 * 1024;
    private static final int DEFAULT_BUFFER_COUNT = 3;
    BlockingQueue<ByteBuffer> writeQueue;
    BlockingQueue<ByteBuffer> readQueue;
    int bufferCapacity;
    int queueSize;
    ByteBuffer currentWriteBuffer;
    ByteBuffer currentReadBuffer;

    public RecordBuffer(int capacity, int queueSize) {
        LOG.info("init RecordBuffer capacity {}, count {}", capacity, queueSize);
        Preconditions.checkState(capacity > 0);
        Preconditions.checkState(queueSize > 1);
        writeQueue = new ArrayBlockingQueue<>(queueSize);
        for(int index = 0; index < queueSize; index++) {
            writeQueue.add(ByteBuffer.allocate(capacity));
        }
        readQueue = new LinkedBlockingDeque<>();
        this.bufferCapacity = capacity;
        this.queueSize = queueSize;
    }

    public RecordBuffer() {
        this(DEFAULT_CAPACITY, DEFAULT_BUFFER_COUNT);
    }

    public void startBufferData() {
        LOG.info("start buffer data, read queue size {}, write queue size {}", readQueue.size(), writeQueue.size());
        Preconditions.checkState(readQueue.size() == 0);
        Preconditions.checkState(writeQueue.size() == queueSize);
        for(ByteBuffer byteBuffer: writeQueue) {
            Preconditions.checkState(byteBuffer.position() == 0);
            Preconditions.checkState(byteBuffer.remaining() == bufferCapacity);
        }
    }

    public void stopBufferData() throws IOException{
        try {
            // add Empty buffer as finish flag.
            boolean isEmpty = false;
            if (currentWriteBuffer != null) {
                currentWriteBuffer.flip();
                // check if the current write buffer is empty.
                isEmpty = currentWriteBuffer.limit() == 0;
                readQueue.put(currentWriteBuffer);
                currentWriteBuffer = null;
            }
            if (!isEmpty) {
                ByteBuffer byteBuffer = writeQueue.take();
                byteBuffer.flip();
                Preconditions.checkState(byteBuffer.limit() == 0);
                readQueue.put(byteBuffer);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
    public void write(byte[] buf) throws InterruptedException{

        int wPos = 0;
        do {
            if (currentWriteBuffer == null) {
                currentWriteBuffer = writeQueue.take();
            }
            int available = currentWriteBuffer.remaining();
            int nWrite = Math.min(available, buf.length - wPos);
            currentWriteBuffer.put(buf, wPos, nWrite);
            wPos += nWrite;
            if (currentWriteBuffer.remaining() == 0) {
                currentWriteBuffer.flip();
                readQueue.put(currentWriteBuffer);
                currentWriteBuffer = null;
            }
        } while (wPos != buf.length);
    }

    public int read(byte[] buf) throws InterruptedException {
        if (currentReadBuffer == null) {
            currentReadBuffer = readQueue.take();
        }
        // Empty buffer as end flag
        if(currentReadBuffer.limit() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
            Preconditions.checkState(readQueue.size() == 0);
            return -1;
        }
        int available = currentReadBuffer.remaining();
        int nRead = Math.min(available, buf.length);
        currentReadBuffer.get(buf, 0, nRead);
        if(currentReadBuffer.remaining() == 0) {
            recycleBuffer(currentReadBuffer);
            currentReadBuffer = null;
        }
        return nRead;
    }

    private void recycleBuffer(ByteBuffer buffer) throws InterruptedException {
        buffer.clear();
        writeQueue.put(buffer);
    }

    public int getWriteQueueSize() {
        return writeQueue.size();
    }

    public int getReadQueueSize() {
        return readQueue.size();
    }
}
