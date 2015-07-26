package co.elastic.logstash;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.util.UUID;

/**
 * Unit test for simple LogstashChronicleQueue.
 */
public class LogstashChronicleQueueTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public LogstashChronicleQueueTest(String testName)
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( LogstashChronicleQueueTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testQueue()
    {
        try {
            String basePath = "/tmp/logstash-chronicle-tests/" + UUID.randomUUID();

            LogstashChronicleQueue q = new LogstashChronicleQueue(basePath);
            LogstashChronicleQueue.Appender appender = q.getAppender(1);
            LogstashChronicleQueue.Tailer tailer = q.getTailer();


            int count = 100;

            for (int i = 0; i<count; i++) {
                String input = "Hello " + i;
                appender.push(input.getBytes());
            }

            for (int i=0; i<count; i++) {
                String input = "Hello " + i;
                byte[] output = tailer.pop();

                assertEquals(new String(output), input);
            }

            q.chronicle.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertTrue(true);
    }

    public void testResume() {
        String basePath = "/tmp/logstash-chronicle-tests/" + UUID.randomUUID();

        LogstashChronicleQueue q = null;
        try {
            q = new LogstashChronicleQueue(basePath);
            LogstashChronicleQueue.Appender appender = q.getAppender();
            LogstashChronicleQueue.Tailer tailer = q.getTailer();

            for (int i=0; i<50; i++) {
                appender.push(("hello " + i).getBytes());
            }

            for (int i=0; i<25; i++) {
                tailer.deq();
                tailer.ack();
            }
            //q.close();

            LogstashChronicleQueue newQ = new LogstashChronicleQueue(basePath);
            LogstashChronicleQueue.Tailer newTailer = newQ.getTailer();
            assertEquals(new String(tailer.pop()), "hello 25");


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
