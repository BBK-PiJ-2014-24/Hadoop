package readerTests;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;


import reader.Map_Reader;
import utilities.Node;

public class mapReader_Test {
	
	
	// Fields
	// ------
	
	MapDriver<IntWritable, IntWritable, IntWritable, Node> mapDriver;
	Map_Reader myMapper;
	
	@Before
	public void setUp(){
		mapDriver = new MapDriver<IntWritable, IntWritable, IntWritable, Node>();
		myMapper = new Map_Reader();
		mapDriver.setMapper(myMapper);
	}
	
	
	public void test1() throws IOException{
		mapDriver.withInput(new IntWritable(2), new IntWritable(2));
		mapDriver.runTest();
	}
	
}
