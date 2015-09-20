import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;
import org.junit.Before;
import org.junit.Assert.*;
import chainMaps.Map5;



public class Map_RankCountTest {

	// Fields
	// ------
	MapDriver<Text, IntWritable, NullWritable, Text> mapDriver;
	Map5 myMapper;
	
	
	
	
	@Before
	public void setUp(){
		mapDriver = new MapDriver<Text, IntWritable, NullWritable, Text>();
		myMapper = new Map5();
		mapDriver.setMapper(myMapper);
		ClassLoader classLoader = getClass().getClassLoader();
	//	File inputFile = new File(classLoader.getResource("./src/test/resources/input.txt").getFile());
	//	File outputFile = new File(classLoader.getResource("./src/test/resources/output.txt").getFile());
	
	}
	
	
	@Test
	public void mapRankTest1() throws IOException{
		
		// Text value = new Text(inputFile);
        //Text output = new Text(outputFile);
			//Scanner scanner = new Scanner(inputFile);
		
		NullWritable n = NullWritable.get();
			//Text value =Text 
		
		 //  mapDriver.addAll(TestUtilities.getOutputRecords());             
		 //  mapDriver.withOutput(NullWritable.get(), new Text(output));

		 //  mapDriver.runTest();
		
		mapDriver.addInput(new Text("one"), new IntWritable(1));
		mapDriver.addInput(new Text("two"), new IntWritable(2));
		mapDriver.addInput(new Text("three"), new IntWritable(3));
		mapDriver.addInput(new Text("four"), new IntWritable(4));
		mapDriver.addInput(new Text("five"), new IntWritable(5));
		
		mapDriver.withOutput(n, new Text("five	5"));
		mapDriver.withOutput(n, new Text("four	4"));
		mapDriver.withOutput(n, new Text("three	3"));
		mapDriver.runTest();
		
	}
	
}
