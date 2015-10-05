package basicTests;

/**
 *  Tests Reduce_Basic
*/


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import basic.Mapper_Basic;
import basic.Reduce_Basic;
import utilities.IntWritableArray;
import utilities.IntWritableArray2;
import utilities.Node;
import utilities.NodeType;



public class Reduce_Basic_Test {

	
	
	// Fields
		// ------
		
		ReduceDriver<IntWritable, Node, IntWritable, Node> reduceDriver;
		Reduce_Basic myReducer;
		Node inputNode1;
		Node inputNode2;
		Node inputNode3;
		Node inputNode4;
		List<Node> nodeList;
		
		
		Node outputNode1;
		Node outputNode1b;
		Node outputNode2;
		Node outputNode3;
		Node outputNode4;
		Node outputNode5;
		Node outputNode6;
		int[] intArr;
		int[] emptyIntArr;
		
		
		
		
		@Before
		public void setUp(){
			reduceDriver = new ReduceDriver<IntWritable, Node, IntWritable, Node>();
			myReducer = new Reduce_Basic();
			reduceDriver.setReducer(myReducer);
			
			intArr = new int[]{105,106,107};
			emptyIntArr = new int[]{};
		
			inputNode1 = new Node();
			inputNode1.setNodeType(NodeType.ProbMass);
			inputNode1.setNodeID(101);
			inputNode1.setPageRank(0.1f);
			
			
			inputNode2 = new Node();
			inputNode2.setNodeType(NodeType.ProbMass);
			inputNode2.setNodeID(101);
			inputNode2.setPageRank(0.2f);
			
			
			inputNode3 = new Node();
			inputNode3.setNodeType(NodeType.ProbMass);
			inputNode3.setNodeID(101);
			inputNode3.setPageRank(0.3f);
			
			inputNode4 = new Node();
			inputNode4.setNodeType(NodeType.AdjList);
			inputNode4.setNodeID(101);
			inputNode4.setAdjList(intArr);
					
			nodeList = new ArrayList<Node>();
			nodeList.add(inputNode1);
			nodeList.add(inputNode2);
			nodeList.add(inputNode3);
			nodeList.add(inputNode4);
			
			outputNode1 = new Node();
			outputNode1.setNodeType(NodeType.CompleteStructure);
			outputNode1.setNodeID(101);
			outputNode1.setPageRank(0.6f);
			outputNode1.setAdjList(intArr);
			
			
			

		}
		
		@Test
		public void reduceTest1() throws IOException{
			
			reduceDriver.withInput(new IntWritable(101), nodeList);
			reduceDriver.withOutput(new IntWritable(101), outputNode1);
			reduceDriver.runTest();
		}
		
		
		
}
