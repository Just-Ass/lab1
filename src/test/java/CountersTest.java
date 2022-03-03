import bdtc.lab1.MessageType;
import com.sun.xml.internal.org.jvnet.mimepull.MIMEMessage;
import eu.bitwalker.useragentutils.UserAgent;
//import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

//    private final String testMalformedIP = "mama mila ramu";
    private final String testMessage = "something went wrong WARN";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

//    @Test
//    public void testMapperCounterOne() throws IOException  {
//        mapDriver
//                .withInput(new LongWritable(), new Text(testMalformedIP))
//                .runTest();
//        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
//                .findCounter(CounterType.MALFORMED).getValue());
//    }

    @Test
    public void testMapperCounterZero() throws IOException {
        MessageType message = HW1Mapper.getTypeMessage(testMessage);
        mapDriver
                .withInput(new LongWritable(), new Text(testMessage))
                .withOutput(new Text(message.toString()), new IntWritable(1))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(MessageType.WARN).getValue());
    }

//    @Test
//    public void testMapperCounters() throws IOException {
//        MessageType message = HW1Mapper.getTypeMessage(testMessage);
//        mapDriver
//                .withInput(new LongWritable(), new Text(testMessage))
//                .withInput(new LongWritable(), new Text(testMalformedIP))
//                .withInput(new LongWritable(), new Text(testMalformedIP))
//                .withOutput(new Text(userAgent.getBrowser().getName()), new IntWritable(1))
//                .runTest();
//
//        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
//                .findCounter(CounterType.MALFORMED).getValue());
//    }
}

