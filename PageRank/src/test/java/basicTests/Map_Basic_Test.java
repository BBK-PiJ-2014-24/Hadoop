/**
 * Tests Map_Basic. Tests it can emit from a completeStructure Node a AdjList Node 
 * and also emit a node's prob rank through to its adjList.
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
	Node inputNode1;
	Node inputNode2;
	
	Node outputNode1;
	Node outputNode1b;
	Node outputNode2;
	Node outputNode3;
	Node outputNode4;
	Node outputNode5;
	Node outputNode6;
	int[] intArr;
	int[] emptyIntArr;
	IntWritableArray2 writArr;
	
	
	
	@Before
	public void setUp(){
		mapDriver = new MapDriver<IntWritable, Node, IntWritable, Node>();
		myMapper = new Mapper_Basic();
		mapDriver.setMapper(myMapper);
		
		intArr = new int[]{105,106,107};
		emptyIntArr = new int[]{};
	
		inputNode1 = new Node();
		inputNode1.setNodeType(NodeType.CompleteStructure);
		inputNode1.setNodeID(101);
		inputNode1.setPageRank(0.33f);
		inputNode1.setAdjList(intArr);
		
		inputNode2 = new Node();
		inputNode2.setNodeType(NodeType.ProbMass);
		inputNode2.setNodeID(102);
		inputNode2.setPageRank(0.33f);
		inputNode2.setAdjList(emptyIntArr);
			
	
		outputNode1 = new Node();
		outputNode2 = new Node();
		outputNode3 = new Node();
		outputNode4 = new Node();
		outputNode5 = new Node();
		outputNode6 = new Node();
		
	}
	
	
	/**
	 * Test Normal Map functions with NodeType.CompleteStructure
	 * Node.adjList
	 * @throws IOException
	 */
	
	@Test
	public void testMap() throws IOException{

		outputNode1.setNodeType(NodeType.AdjList);
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
		
		mapDriver.withInput(new IntWritable(101),inputNode1);
		mapDriver.withOutput(new IntWritable(101),outputNode1);
		mapDriver.withOutput(new IntWritable(105),outputNode2);
		mapDriver.withOutput(new IntWritable(106),outputNode3);
		mapDriver.withOutput(new IntWritable(107),outputNode4);
		mapDriver.runTest();
		
		System.out.println("hello");
		
	}
	
	/**
	 * Test for NodeType.ProbMass
	 */
	
	public void mapTest2(){
		
		outputNode1b.setNodeType(NodeType.ProbMass);
		outputNode1b.setNodeID(102);
		outputNode1b.setPageRank(0.33f);
		
		mapDriver.withInput(new IntWritable(102),inputNode2);
		mapDriver.withOutput(new IntWritable(102),outputNode1b);
	}
	
	
	
}
