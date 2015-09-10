import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;
import org.junit.Before;





public class MapperTest {

	// Fields
	// ------
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	
	
	
	@Before
	public void setUp(){
		mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
		ConditionalWordCount_Mapper myMapper = new ConditionalWordCount_Mapper();
		mapDriver.setMapper(myMapper);
	}
	
	
	
	@Test
	public void testMap1() throws IOException, InterruptedException {
	 Text value = new Text("Rain Spain Rain");
	                     
	   mapDriver.withInput(new LongWritable(1), value);
	   mapDriver.withOutput(new Text("Rain"),new IntWritable(1));
	   mapDriver.withOutput(new Text("Spain"),new IntWritable(1));
	   mapDriver.withOutput(new Text("Rain"), new IntWritable(1));
	   mapDriver.runTest();
	}
	
}
	
	
	
	
