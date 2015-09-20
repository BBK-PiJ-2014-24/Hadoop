/**
 * Tester for PairReducer
 */
		

package pairTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import pairs.Pair_Reducer;

public class ReduceTester {
	 
	// Fields
	// ------ 
	 ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	 Pair_Reducer myReducer;
	
	
	
	  @Before
	  public void setUp(){
		  myReducer = new Pair_Reducer();
		  reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		  reduceDriver.setReducer(myReducer);
	  }
	
	
	
	  @Test
	  public void testReducer1() throws IOException{
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));  
	    values.add(new IntWritable(1));   // so values = {1,1}
	    reduceDriver.withInput(new Text("for rain"), values);
	    reduceDriver.withOutput(new Text("for rain"), new IntWritable(2));
	    reduceDriver.runTest();
	  }
}
