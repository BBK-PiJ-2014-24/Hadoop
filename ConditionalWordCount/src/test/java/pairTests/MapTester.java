package pairTests;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import aggregate.ConditionalWordCount_Mapper;
import pairs.Pair_Mapper;

public class MapTester {

	// Fields
		// ------
		MapDriver<Object, Text, Text, IntWritable> mapDriver;
		Pair_Mapper myMapper;
		
		
		@Before
		public void setUp(){
			mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
			myMapper = new Pair_Mapper();
			mapDriver.setMapper(myMapper);
		}
		
		
		/**
		 * Tests Simple Word Count
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Test
		public void testMap1() throws IOException, InterruptedException {
		 Text value = new Text("for go  for  rain For Spain\"");
		                     
		   mapDriver.withInput(new LongWritable(1), value);
		   mapDriver.withOutput(new Text("for go"),new IntWritable(1));
		   mapDriver.withOutput(new Text("for rain"),new IntWritable(1));
		   mapDriver.withOutput(new Text("for spain"),new IntWritable(1));
		   mapDriver.runTest();
		}
	
	
}
