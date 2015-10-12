package readerTests;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import reader.Map_Reader;
import reader.Map_Reader2;
import reader.Map_Reader3;
import utilities.Node;
import utilities.NodeType;

public class MapReader_Test2 {
	
	
	// Fields
	// ------
	
	MapDriver<LongWritable, Text, IntWritable, IntWritable> mapDriver;
	Map_Reader3 myMapper;
	Node n;
	Text t;
	Path path;
	
	@Before
	public void setUp(){
		mapDriver = new MapDriver<LongWritable, Text, IntWritable, IntWritable>();
		myMapper = new Map_Reader3();
		mapDriver.setMapper(myMapper);
		String s = "# Directed graph (each unordered pair of nodes is saved once): soc-Epinions1.txt\n" + 
				"# Directed Epinions social network\n" +
				"# Nodes: 75879 Edges: 508837\n" +
				"# FromNodeId    ToNodeId\n" +
				"0	139\n"+
				"0	140\n"+
				"0	141\n"+
				"0	142\n"+
				"0	799\n"+
				"4	   1\n"+
				"4	10\n"+
				"4	12\n"+
				"4	18\n"+
				"4  22\n";
		t = new Text(s);
		
		path =new Path("./src/main/resources/Sample.txt");
	}
	
	@Test
	public void test1() throws IOException{
		
		
		
		mapDriver.withInput(new LongWritable(0), t);
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
