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
		MapDriver<Object, Text, Text, MyCompoundValue> mapDriver;
		MeanMapper myMapper;
		
		
		@Before
		public void setUp(){
			mapDriver = new MapDriver<Object, Text, Text, MyCompoundValue>();
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
		                     
		   mapDriver.withInput(new LongWritable(1),new Text("Stewart	2"));
		   mapDriver.withInput(new LongWritable(1), new Text("Stewart	3"));
		   mapDriver.withInput(new LongWritable(1), new Text("Stewart	4"));
		   mapDriver.withInput(new LongWritable(1), new Text("Mike	4"));
		   mapDriver.withInput(new LongWritable(1), new Text("Mike	5"));
		   mapDriver.withInput(new LongWritable(1), new Text("Mike	6"));
		   mapDriver.withInput(new LongWritable(1), new Text("Rob	7"));
		   mapDriver.withInput(new LongWritable(1), new Text("Rob	8"));
		   mapDriver.withInput(new LongWritable(1), new Text("Rob	9"));
		   
		   
		   mapDriver.withOutput(new Text("Stewart"),new MyCompoundValue(2,1));
		   mapDriver.withOutput(new Text("Stewart"),new MyCompoundValue(3,1));
		   mapDriver.withOutput(new Text("Stewart"),new MyCompoundValue(4,1));
		   mapDriver.withOutput(new Text("Mike"),new MyCompoundValue(4,1));
		   mapDriver.withOutput(new Text("Mike"),new MyCompoundValue(5,1));
		   mapDriver.withOutput(new Text("Mike"),new MyCompoundValue(6,1));
		   mapDriver.withOutput(new Text("Rob"),new MyCompoundValue(7,1));
		   mapDriver.withOutput(new Text("Rob"),new MyCompoundValue(8,1));
		   mapDriver.withOutput(new Text("Rob"),new MyCompoundValue(9,1));
		  
		
		   mapDriver.runTest();
		}
}
