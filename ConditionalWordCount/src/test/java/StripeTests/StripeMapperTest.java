package StripeTests;



import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


import stripes.Stripe_Mapper;
import utilities.MyMapWritable;
import utilities.MyMapWritable2;


public class StripeMapperTest {

	// Fields
			// ------
			MapDriver<Object, Text, Text, MyMapWritable2> mapDriver;
			Stripe_Mapper myMapper;
			Text value;
			String ans;
			
			
			@Before
			public void setUp(){
				mapDriver = new MapDriver<Object, Text, Text, MyMapWritable2>();
				myMapper = new Stripe_Mapper();
				mapDriver.setMapper(myMapper);
				value = new Text("for go  for  rain For Spain\" for rain");
				ans = "go\t1\nspain\t1\nrain\t2\n";
			}
			
			
			/**
			 * Tests with mapDriver.run(), which runs output 
			 * and the results passed to a list.
			 * Output is then checked manually.
			 * @throws IOException
			 * @throws InterruptedException
			 */
			@Test
			public void testMap1() throws IOException, InterruptedException {
			 
                  
			  mapDriver.withInput(new LongWritable(1), value);
			
			  List<Pair<Text,MyMapWritable2>> list =  mapDriver.run();
			  Text t = list.get(0).getFirst();
			  String s = list.get(0).getSecond().toString();
			  System.out.println(t);
			  System.out.println(s);
			  assertEquals("Test Output", ans, s);
			   
			} 
			
		


			@Test
			public void test2(){
				
				// MapWritable2 mw = new MapWritable();
				// List<Pair<Text,IntWritable>>myOutput = new ArrayList<Pair<Text,IntWritable>>();
				 
				//  myOutput.add(new Pair<Text,IntWritable>(new Text("go"), new IntWritable(1)));
				 // myOutput.add(new Pair<Text,IntWritable>(new Text("rain"), new IntWritable(2)));
				 // myOutput.add(new Pair<Text,IntWritable>(new Text("spain"), new IntWritable(1)));
				 //mw.put(new Text("rain"), new IntWritable(2));
				 //mw.put(new Text("spain"), new IntWritable(1));
				
				  //mapDriver.withAllOutput(new Text("for"), myOutput(mw));
			}
}
		