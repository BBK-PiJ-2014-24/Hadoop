
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;
import org.junit.Before;


public class ReducerTest {

	 // Fields
	 // ------
	 
	 ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	 ConditionalWordCount_Reducer myReducer;
	
	
	
	  @Before
	  public void setUp(){
		  myReducer = new ConditionalWordCount_Reducer();
		  reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		  reduceDriver.setReducer(myReducer);
	  }
	
	
	
	  @Test
	  public void testReducer1() throws IOException{
	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));  
	    values.add(new IntWritable(1));   // so values = {1,1}
	    reduceDriver.withInput(new Text("cat"), values);
	    reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
	    reduceDriver.runTest();
	  }
}