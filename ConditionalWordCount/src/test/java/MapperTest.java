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
		assertEquals("Test \"", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("Hello'");
		assertEquals("Test '", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("Hello-");
		assertEquals("Test -", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("\"Hello");
		assertEquals("Test \" at beginning", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("'Hello");
		assertEquals("Test ' at beginning", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("-Hello");
		assertEquals("Test - at beginning", new Text("Hello"), myMapper.screenPunctuation(word));
		word.set("\"Hello\"");
		assertEquals("Test \" at beginning and end ", new Text("Hello"), myMapper.screenPunctuation(word));
	}
	
	
	/**
	 * Test for the incorporation of screenPunctuation() in Mapper
	 * @throws IOException
	 * @throws InterruptedException
	 */

	@Test
	public void testMap4() throws IOException, InterruptedException{
		Text value = new Text(" 'Rain \"Spain rain. falls! \"plain\"");
		mapDriver.withInput(new LongWritable(1), value);
		mapDriver.withOutput(new Text("rain"),new IntWritable(1));
		mapDriver.withOutput(new Text("spain"),new IntWritable(1));
		mapDriver.withOutput(new Text("rain"), new IntWritable(1));
		mapDriver.withOutput(new Text("falls"), new IntWritable(1));
		mapDriver.withOutput(new Text("plain"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	/**
	 * Test for screenStem()
	 */
	@Test
	public void testMap6(){
		
		Text word = new Text("orderly");
		assertEquals("Test suffix -ly", new Text("order"), ConditionalWordCount_Mapper.screenStem(word));
		word.set("ordering");
		assertEquals("Test suffix -ing", new Text("order"), ConditionalWordCount_Mapper.screenStem(word));
		word.set("ordered");
		assertEquals("Test suffix -ed", new Text("order"), ConditionalWordCount_Mapper.screenStem(word));
		word.set("ring");
		assertEquals("Test suffix r-ing", new Text("ring"), ConditionalWordCount_Mapper.screenStem(word));
		word.set("fling");
		assertEquals("Test suffix fl-ing", new Text("fling"), ConditionalWordCount_Mapper.screenStem(word));
		word.set("fly");
		assertEquals("Test suffix -ed", new Text("fly"), ConditionalWordCount_Mapper.screenStem(word));
		word.set("a");
		assertEquals("Test word a", new Text("a"), ConditionalWordCount_Mapper.screenStem(word));
	
	}
	
	
	/**
	 * Test screemStem in the Mapper
	 */
	@Test
	public void testMap7(){
		Text value = new Text("rain raining rained orderly");
		mapDriver.withInput(new LongWritable(1), value);
		mapDriver.withOutput(new Text("rain"),new IntWritable(1));
		mapDriver.withOutput(new Text("rain"),new IntWritable(1));
		mapDriver.withOutput(new Text("rain"), new IntWritable(1));
		mapDriver.withOutput(new Text("order"), new IntWritable(1));
	}	

}
	
	
	
	
