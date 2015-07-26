package co.elastic.logstash;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.constraints.MaxSize;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class LogstashChronicleQueue
{
    final Chronicle chronicle;
    final Chronicle tracker;
    long resumeOffset;

    LogstashChronicleQueue(String basePath) throws IOException {
        chronicle = ChronicleQueueBuilder.indexed(basePath + "/queue").build();
        tracker = ChronicleQueueBuilder.indexed(basePath + "/tracker").build();
        resumeOffset = calculateResumeOffset();
    }

    private long calculateResumeOffset() throws IOException {
        // TODO: Figure out efficient way to seek directly to end
        ExcerptTailer tailer = tracker.createTailer();
        tailer.toStart();

        long v = 0;
        while (tailer.nextIndex()) {
            v = tailer.readLong();
            tailer.finish();
        }

        return v;
    }

    public void close() throws IOException {
        chronicle.close();
    }

    public long getResumeOffset() {
        return resumeOffset;
    }


    public Chronicle getChronicle() {
        return chronicle;
    }

    public Appender getAppender() throws IOException {
        return new Appender(this, 1048576); // 1MB default excerpt size
    }

    public Appender getAppender(long excerptSize) throws IOException {
        return new Appender(this, excerptSize);
    }

    public Tailer getTailer() throws IOException {
        return new Tailer(this);
    }

    public Chronicle getTracker() {
        return tracker;
    }

    public class Appender {
        final LogstashChronicleQueue lcq;
        final Chronicle chronicle;
        final ExcerptAppender appender;
        final long excerptSize;

        Appender(LogstashChronicleQueue lcq_, long excerptSize_) throws IOException {
            lcq = lcq_;
            chronicle = lcq.getChronicle();

            appender = chronicle.createAppender();
            excerptSize = excerptSize_;
        }

        public void push(byte[] item) {
            appender.startExcerpt();
            appender.nextSynchronous(true); // Is this even necessary?
            appender.writeObject(item);
            appender.finish();
        }
    }

    public class Tailer {
        final LogstashChronicleQueue lcq;
        final Chronicle chronicle;
        final ExcerptTailer tailer;
        final ExcerptAppender trackerAppender;

        Tailer(LogstashChronicleQueue lcq_) throws IOException {
            lcq = lcq_;
            chronicle = lcq.getChronicle();
            tailer = chronicle.createTailer();
            trackerAppender = lcq.getTracker().createAppender();

            // These are always the same value. Probably because a long is a fixed len?
            tailer.limit(lcq.getResumeOffset());
            tailer.position(lcq.getResumeOffset());
        }

        public byte[] deq() {
            return pop();
        }

        public byte[] pop() {
            while(!tailer.nextIndex());
            return (byte[]) tailer.readObject();
        }

        public void ack() {
            trackerAppender.startExcerpt(8);
            trackerAppender.writeLong(tailer.position());
            trackerAppender.finish();
            tailer.finish();
        }
    }
}
