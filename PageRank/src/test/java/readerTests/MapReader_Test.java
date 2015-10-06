package readerTests;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import reader.Map_Reader;
import utilities.Node;
import utilities.NodeType;

public class MapReader_Test {
	
	
	// Fields
	// ------
	
	MapDriver<IntWritable, IntWritable, IntWritable, IntWritable> mapDriver;
	Map_Reader myMapper;
	Node n;
	
	@Before
	public void setUp(){
		mapDriver = new MapDriver<IntWritable, IntWritable, IntWritable, IntWritable>();
		myMapper = new Map_Reader();
		mapDriver.setMapper(myMapper);
	}
	
	@Test
	public void test1() throws IOException{
		n = new Node();
		n.setNodeType(NodeType.ProbMass);
		n.setPageRank(0.33f);
		
		mapDriver.withInput(new IntWritable(0), new IntWritable(2));
		mapDriver.withOutput(new IntWritable(0), new IntWritable(139));
		mapDriver.withOutput(new IntWritable(0), new IntWritable(140));
		mapDriver.withOutput(new IntWritable(0), new IntWritable(141));
		mapDriver.withOutput(new IntWritable(0), new IntWritable(142));
		mapDriver.withOutput(new IntWritable(0), new IntWritable(799));
		mapDriver.withOutput(new IntWritable(4), new IntWritable(1));
		mapDriver.withOutput(new IntWritable(4), new IntWritable(10));
		mapDriver.withOutput(new IntWritable(4), new IntWritable(12));
		mapDriver.withOutput(new IntWritable(4), new IntWritable(18));
		mapDriver.withOutput(new IntWritable(4), new IntWritable(22));
		mapDriver.runTest();
	}
	
}
