/**
 * Tests Map_Basic
 */

package basicTests;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import basic.Mapper_Basic;
import utilities.IntWritableArray;
import utilities.IntWritableArray2;
import utilities.Node;
import utilities.NodeType;




public class Map_Basic_Test {

	// Fields
	// ------
	
	MapDriver<IntWritable, Node, IntWritable, Node> mapDriver;
	Mapper_Basic myMapper;
	Node inputNode;
	Node outputNode1;
	Node outputNode2;
	Node outputNode3;
	Node outputNode4;
	Node outputNode5;
	int[] intArr;
	IntWritableArray2 writArr;
	
	
	
	@Before
	public void setUp(){
		mapDriver = new MapDriver<IntWritable, Node, IntWritable, Node>();
		myMapper = new Mapper_Basic();
		mapDriver.setMapper(myMapper);
		
		intArr = new int[]{105,106,107};
		
	
		inputNode = new Node();
		inputNode.setNodeType(NodeType.Structure);
		inputNode.setNodeID(101);
		inputNode.setPageRank(0.33f);
		inputNode.setAdjList(intArr);
	
		outputNode1 = new Node();
		outputNode2 = new Node();
		outputNode3 = new Node();
		outputNode4 = new Node();
		outputNode5 = new Node();
		
	}
	
	
	
	
	@Test
	public void testMap() throws IOException{

		outputNode1.setNodeType(NodeType.Structure);
		outputNode1.setNodeID(101);
		outputNode1.setAdjList(intArr);
		outputNode1.setPageRank(0.33f);
		
		outputNode2.setNodeType(NodeType.ProbMass);
		outputNode2.setNodeID(105);
		outputNode2.setPageRank(0.11f);
		
		outputNode3.setNodeType(NodeType.ProbMass);
		outputNode3.setNodeID(106);
		outputNode3.setPageRank(0.11f);
		
		outputNode4.setNodeType(NodeType.ProbMass);
		outputNode4.setNodeID(107);
		outputNode4.setPageRank(0.11f);
		
		
		
		mapDriver.withInput(new IntWritable(101),inputNode);
		mapDriver.withOutput(new IntWritable(101),outputNode1);
		mapDriver.withOutput(new IntWritable(105),outputNode2);
		mapDriver.withOutput(new IntWritable(106),outputNode3);
		mapDriver.withOutput(new IntWritable(107),outputNode4);
		mapDriver.runTest();
		
		System.out.println("hello");
		
	}
	
	
	
}
