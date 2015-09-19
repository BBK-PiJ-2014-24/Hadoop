package mean;


import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;
import mean.MeanMapper;

import org.junit.Assert.*;



public class MeanTeamTester {

	
	// Fields
		// ------
		MapDriver<Object, Text, Text, IntWritable> mapDriver;
		MeanMapper myMapper;
		
		
		@Before
		public void setUp(){
			mapDriver = new MapDriver<Object, Text, Text, IntWritable>();
			myMapper = new MeanMapper();
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
		                     
		   mapDriver.withInput(new Text("Stewart"), new IntWritable(2));
		   mapDriver.withInput(new Text("Stewart"), new IntWritable(3));
		   mapDriver.withInput(new Text("Stewart"), new IntWritable(3));
		   mapDriver.withInput(new Text("Mike"), new IntWritable(4));
		   mapDriver.withInput(new Text("Mike"), new IntWritable(5));
		   mapDriver.withInput(new Text("Mike"), new IntWritable(6));
		   mapDriver.withInput(new Text("Rob"), new IntWritable(7));
		   mapDriver.withInput(new Text("Rob"), new IntWritable(8));
		   mapDriver.withInput(new Text("Rob"), new IntWritable(9));
		   
		   mapDriver.withOutput(new Text("Mike"),new IntWritable(5));
		   mapDriver.withOutput(new Text("Rob"),new IntWritable(8));
		   mapDriver.withOutput(new Text("Stewart"), new IntWritable(2));
		   mapDriver.runTest();
		}
}
