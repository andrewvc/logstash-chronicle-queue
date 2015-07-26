package co.elastic.logstash;

import java.io.IOException;
import java.util.Queue;

/**
 * Created by andrewvc on 7/26/15.
 */
public class LogstashRotatingChronicleQueue {
    final String basePath;
    long maxBytes;
    volatile long totalQueueBytesWritten = 0;
    volatile long currentQueueBytesWritten = 0;
    volatile LogstashChronicleQueue currentQueue;
    volatile LogstashChronicleQueue.Tailer currentTailer;
    volatile LogstashChronicleQueue.Appender currentAppender;

    public static String version() {
        return "1.0.0";
    }

    public LogstashRotatingChronicleQueue(long maxBytes_, String basePath_) throws IOException {
        maxBytes = maxBytes_;
        basePath = basePath_;

        // TODO: Write resume functionality, detect old files
        this.rotateQueue();

    }

    private void rotateQueue() throws IOException {
        this.currentQueue = new LogstashChronicleQueue(basePath);
        this.currentAppender = currentQueue.getAppender();
        this.currentTailer = currentQueue.getTailer();
    }

    private LogstashChronicleQueue currentQueue() {
        return currentQueue;

    }

    // TODO: Add blocking behavior
    synchronized void push(byte[] event) throws IOException {
        totalQueueBytesWritten += event.length;
        currentQueueBytesWritten += event.length;

        if (currentQueueBytesWritten > maxBytes) {
            rotateQueue();
        }

        currentAppender.push(event);
    }

    public byte[] deq() {
        return currentTailer.deq();
    }

    public void ack() {
        currentTailer.ack();
    }
}
