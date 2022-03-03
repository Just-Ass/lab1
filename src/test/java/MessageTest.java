import bdtc.lab1.MessageType;
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


public class MessageTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String testMessage = "something went wrong WARN";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperMessage() throws IOException {
// Если строка содержит "WARN", то тест успешно пройден
        MessageType message = HW1Mapper.getTypeMessage(testMessage);
        mapDriver
                .withInput(new LongWritable(), new Text(testMessage))
                .withOutput(new Text(message.toString()), new IntWritable(1))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(MessageType.WARN).getValue());
    }
}

