
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;
import org.junit.Before;

import chainMaps.Reduce2;


public class Reducer2Test {

	 // Fields
	 // ------
	 
	 ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	 Reduce2 myReducer;
	
	
	
	  @Before
	  public void setUp(){
		  myReducer = new Reduce2();
		  reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		  reduceDriver.setReducer(myReducer);
	  }
	
	
	
	  @Test
	  public void testReducer1() throws IOException{
	   
		// INPUTS  
		List<IntWritable> values1 = new ArrayList<IntWritable>();
	    values1.add(new IntWritable(1)); 
	    reduceDriver.withInput(new Text("one"), values1);
	    
	    List<IntWritable> values2 = new ArrayList<IntWritable>();
	    for(int i=0; i<2; i++)
	    	values2.add(new IntWritable(1)); 
	    reduceDriver.withInput(new Text("two"), values2);
	    
	    List<IntWritable> values3 = new ArrayList<IntWritable>();
	    for(int i=0; i<3; i++)
	    	values3.add(new IntWritable(1)); 
	    reduceDriver.withInput(new Text("three"), values3);
	    
	    List<IntWritable> values4 = new ArrayList<IntWritable>();
	    for(int i=0; i<4; i++)
	    	values4.add(new IntWritable(1)); 
	    reduceDriver.withInput(new Text("four"), values4);
	    
	   
	    reduceDriver.withOutput(new Text("four"), new IntWritable(4));
	    reduceDriver.withOutput(new Text("three"), new IntWritable(3));
	    reduceDriver.withOutput(new Text("two"), new IntWritable(2));
	    
	    reduceDriver.runTest();
	  }
	 
	  
}
