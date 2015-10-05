package basicTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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



public class MapReduce_Basic_Test1 {

	// Fields
	// ------
		
		MapDriver<IntWritable, Node, IntWritable, Node> mapDriver;
		Mapper_Basic myMapper;
		Node inputNode1;
		Node inputNode2;
		Node inputNode3;
		Node inputNode4;
		Node inputNode5;
		
		Node outputNode1a;
		Node outputNode1b;
		Node outputNode1c;
		Node outputNode2a;
		Node outputNode2b;
		Node outputNode2c;
		
		Node outputNode3a;
		Node outputNode3b;
		Node outputNode4a;
		Node outputNode4b;
		Node outputNode5a;
		Node outputNode5b;
		Node outputNode5c;
		Node outputNode5d;
		
		List inputNodeList;
		List outputNodeList; 
		
		int[] intArr1;
		int[] intArr2;
		int[] intArr3;
		int[] intArr4;
		int[] intArr5;
		int[] emptyIntArr;
		IntWritableArray2 writArr;
		
		
		
		@Before
		public void setUp(){
			mapDriver = new MapDriver<IntWritable, Node, IntWritable, Node>();
			myMapper = new Mapper_Basic();
			mapDriver.setMapper(myMapper);
			
			// Initial Node Inputs
			// ------------------
			
			intArr1 = new int[]{102, 104};
			inputNode1 = new Node();
			inputNode1.setNodeType(NodeType.CompleteStructure);
			inputNode1.setNodeID(101);
			inputNode1.setPageRank(0.2f);
			inputNode1.setAdjList(intArr1);
	
			intArr2 = new int[]{103, 105};
			inputNode2 = new Node();
			inputNode2.setNodeType(NodeType.CompleteStructure);
			inputNode2.setNodeID(102);
			inputNode2.setPageRank(0.2f);
			inputNode2.setAdjList(intArr2);
			
			intArr3 = new int[]{104};
			inputNode3 = new Node();
			inputNode3.setNodeType(NodeType.CompleteStructure);
			inputNode3.setNodeID(103);
			inputNode3.setPageRank(0.2f);
			inputNode3.setAdjList(intArr3);
			
			intArr4 = new int[]{105};
			inputNode4 = new Node();
			inputNode4.setNodeType(NodeType.CompleteStructure);
			inputNode4.setNodeID(104);
			inputNode4.setPageRank(0.2f);
			inputNode4.setAdjList(intArr4);
			
			intArr5 = new int[]{101,102,103};
			inputNode5 = new Node();
			inputNode5.setNodeType(NodeType.CompleteStructure);
			inputNode5.setNodeID(105);
			inputNode5.setPageRank(0.2f);
			inputNode5.setAdjList(intArr5);
			
			// Map Outputs
			// -----------
		
			outputNode1a = new Node();
			outputNode1a.setNodeID(101);
			outputNode1a.setNodeType(NodeType.AdjList);
			outputNode1a.setAdjList(intArr1);
			
			outputNode1b = new Node();
			outputNode1b.setNodeID(102);
			outputNode1b.setNodeType(NodeType.ProbMass);
			outputNode1b.setPageRank(0.1f);
			
			outputNode1c = new Node();
			outputNode1c.setNodeID(104);
			outputNode1c.setNodeType(NodeType.ProbMass);
			outputNode1c.setPageRank(0.1f);
			
			
			
			outputNode2a = new Node();
			outputNode2a.setNodeID(102);
			outputNode2a.setNodeType(NodeType.AdjList);
			outputNode2a.setAdjList(intArr2);
			
			outputNode2b = new Node();
			outputNode2b.setNodeID(103);
			outputNode2b.setNodeType(NodeType.ProbMass);
			outputNode2b.setPageRank(0.1f);
			
			outputNode2c = new Node();
			outputNode2c.setNodeID(105);
			outputNode2c.setNodeType(NodeType.ProbMass);
			outputNode2c.setPageRank(0.1f);
			
			
			
			outputNode3a = new Node();
			outputNode3a.setNodeID(103);
			outputNode3a.setNodeType(NodeType.AdjList);
			outputNode3a.setAdjList(intArr3);
			
			outputNode3b = new Node();
			outputNode3b.setNodeID(104);
			outputNode3b.setNodeType(NodeType.ProbMass);
			outputNode3b.setPageRank(0.2f);
			
			
			
			
			outputNode4a = new Node();
			outputNode4a.setNodeID(104);
			outputNode4a.setNodeType(NodeType.AdjList);
			outputNode4a.setAdjList(intArr4);
			
			outputNode4b = new Node();
			outputNode4b.setNodeID(105);
			outputNode4b.setNodeType(NodeType.ProbMass);
			outputNode4b.setPageRank(0.2f);
			
			
			

			outputNode5a = new Node();
			outputNode5a.setNodeID(105);
			outputNode5a.setNodeType(NodeType.AdjList);
			outputNode5a.setAdjList(intArr5);
			
			outputNode5b = new Node();
			outputNode5b.setNodeID(101);
			outputNode5b.setNodeType(NodeType.ProbMass);
			outputNode5b.setPageRank(0.066f);
			
			outputNode5c = new Node();
			outputNode5c.setNodeID(102);
			outputNode5c.setNodeType(NodeType.ProbMass);
			outputNode5c.setPageRank(0.066f);
			
			outputNode5d = new Node();
			outputNode5d.setNodeID(103);
			outputNode5d.setNodeType(NodeType.ProbMass);
			outputNode5d.setPageRank(0.066f);
			
			
			
			inputNodeList = new ArrayList<Node>();
			inputNodeList.add(inputNode1);
			inputNodeList.add(inputNode2);
			inputNodeList.add(inputNode3);
			inputNodeList.add(inputNode4);
			inputNodeList.add(inputNode5);
			
			outputNodeList = new ArrayList<Node>();
				outputNodeList.add(outputNode1a);
				outputNodeList.add(outputNode1b);
				outputNodeList.add(outputNode1c);
				outputNodeList.add(outputNode2a);
				outputNodeList.add(outputNode2b);
				outputNodeList.add(outputNode2c);
				outputNodeList.add(outputNode3a);
				outputNodeList.add(outputNode3b);
				outputNodeList.add(outputNode4a);
				outputNodeList.add(outputNode4b);
				outputNodeList.add(outputNode5a);
				outputNodeList.add(outputNode5b);
				outputNodeList.add(outputNode5c);
				outputNodeList.add(outputNode5d);
			
			
			

			
		}
		
		
		/**
		 * Test Normal Map functions with NodeType.CompleteStructure
		 * Node.adjList
		 * @throws IOException
		 */
		
		@Test
		public void testMap() throws IOException{


			
			mapDriver.withInput(new IntWritable(101),inputNode1);
			mapDriver.withInput(new IntWritable(102),inputNode2);
			mapDriver.withInput(new IntWritable(103),inputNode3);
			mapDriver.withInput(new IntWritable(104),inputNode4);
			mapDriver.withInput(new IntWritable(105),inputNode5);
			
			
			mapDriver.withOutput(new IntWritable(101),outputNode1a);
			mapDriver.withOutput(new IntWritable(102),outputNode1b);
			mapDriver.withOutput(new IntWritable(104),outputNode1c);
			
			mapDriver.withOutput(new IntWritable(102),outputNode2a);
			mapDriver.withOutput(new IntWritable(103),outputNode2b);
			mapDriver.withOutput(new IntWritable(105),outputNode2c);
			
			mapDriver.withOutput(new IntWritable(103),outputNode3a);
			mapDriver.withOutput(new IntWritable(104),outputNode3b);
			
			mapDriver.withOutput(new IntWritable(104),outputNode4a);
			mapDriver.withOutput(new IntWritable(105),outputNode4b);
			
			mapDriver.withOutput(new IntWritable(105),outputNode5a);
			mapDriver.withOutput(new IntWritable(101),outputNode5b);
			mapDriver.withOutput(new IntWritable(102),outputNode5c);
			mapDriver.withOutput(new IntWritable(103),outputNode5d);
					
		
			mapDriver.runTest();
			
			System.out.println("hello");
			
		}
		


	
	
	
}
