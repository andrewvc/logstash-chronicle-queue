package co.elastic.logstash;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Created by andrewvc on 7/26/15.
 */
public class LogstashRotatingChronicleQueue {
    final String basePath;
    int maxItems;
    volatile long totalQueueItemsWritten = 0;
    volatile int currentQueueItemsWritten = 0;
    volatile long totalQueueBytesWritten = 0;
    volatile long currentQueueBytesWritten = 0;
    volatile LogstashChronicleQueue currentQueue;
    volatile LogstashChronicleQueue.Tailer currentTailer;
    volatile LogstashChronicleQueue.Appender currentAppender;
    final ArrayBlockingQueue<Object> memoryQueue;
    final Converter converter;

    public static String version() {
        return "1.0.0";
    }

    public <T> LogstashRotatingChronicleQueue(int maxItems_, String basePath_, Converter<T> converter_) throws IOException {
        maxItems = maxItems_;
        basePath = basePath_;
        converter = converter_;

        memoryQueue = new ArrayBlockingQueue<Object>(maxItems);

        // TODO: Write resume functionality, detect old files
        this.rotateQueue();

    }

    private void rotateQueue() throws IOException {
        this.currentQueue = new LogstashChronicleQueue(basePath);
        this.currentAppender = currentQueue.getAppender();
        this.currentTailer = currentQueue.getTailer();

        currentQueueItemsWritten = 0;
        currentQueueBytesWritten = 0;
    }

    private LogstashChronicleQueue currentQueue() {
        return currentQueue;

    }

    synchronized void push(Object event) throws IOException, InterruptedException {
        memoryQueue.put(event);

        // Rotate the queue to free up some disk
        // TODO: Figure out a more optimal way to calculate when to do this
        // This is probably worth tuning.
        if (currentQueueItemsWritten > maxItems) {
            rotateQueue();
        }

        byte[] serializedEvent = converter.serialize(event);
        currentAppender.push(serializedEvent);

        totalQueueBytesWritten += serializedEvent.length;
        currentQueueBytesWritten += serializedEvent.length;
    }

    public Object deq() throws InterruptedException {
        return memoryQueue.take();
    }

    private Object deqChronicle() {
        return converter.deserialize(currentTailer.deq());
    }

    public void ack() {
        currentTailer.ack();
    }
}
