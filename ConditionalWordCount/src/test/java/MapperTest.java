import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;
import org.junit.Before;
import org.junit.Assert.*;





public class MapperTest {

	// Fields
	// ------
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ConditionalWordCount_Mapper myMapper;
	
	
	@Before
	public void setUp(){
		mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
		myMapper = new ConditionalWordCount_Mapper();
		mapDriver.setMapper(myMapper);
	}
	
	
	/**
	 * Tests Simple Word Count
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testMap1() throws IOException, InterruptedException {
	 Text value = new Text("rain spain rain");
	                     
	   mapDriver.withInput(new LongWritable(1), value);
	   mapDriver.withOutput(new Text("rain"),new IntWritable(1));
	   mapDriver.withOutput(new Text("spain"),new IntWritable(1));
	   mapDriver.withOutput(new Text("rain"), new IntWritable(1));
	   mapDriver.runTest();
	}
	
	/**
	 * Test for upper-case conversion
	 * @throws IOException
	 * @throws InterruptedException
	 */
	
	@Test
	public void testMap2() throws IOException, InterruptedException{
		Text value = new Text("Rain Spain rain");
		mapDriver.withInput(new LongWritable(1), value);
		mapDriver.withOutput(new Text("rain"),new IntWritable(1));
		mapDriver.withOutput(new Text("spain"), new IntWritable(1));
		mapDriver.withOutput(new Text("rain"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	
	@Test 
	public void testMap3(){
		Text word = new Text("Hello.");
		assertEquals("Test Full Stop", new Text("Hello"), myMapper.screenPunctuation(word));		
		word.set("Hello!");
		assertEquals("Test !", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("Hello\"");
		assertEquals("Test !", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("Hello'");
		assertEquals("Test !", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("Hello-");
		assertEquals("Test !", new Text("Hello"), myMapper.screenPunctuation(word));
	}
	
	
/*
	@Test
	public void testMap4() throws IOException, InterruptedException{
		Text value = new Text("Rain, Spain, rain., falls!");
		mapDriver.withInput(new LongWritable(1), value);
		mapDriver.withOutput(new Text("rain"),new IntWritable(1));
		mapDriver.withOutput(new Text("spain"),new IntWritable(1));
		mapDriver.withOutput(new Text("rain"), new IntWritable(1));
		mapDriver.withOutput(new Text("falls"), new IntWritable(1));
		mapDriver.runTest();
	}	
*/
}
	
	
	
	
