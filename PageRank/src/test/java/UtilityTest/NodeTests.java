/**
 * Test the Node Class
 */

package UtilityTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import utilities.IntWritableArray;
import utilities.Node;
import utilities.NodeType;

public class NodeTests {


	private Node node;
	private IntWritableArray intArr;
	private IntWritable[] arr;
	
	
	@Before
	public void setUp(){
		intArr = new IntWritableArray();
		
		arr = new IntWritable[3];
		arr[0] = new IntWritable(10);
		arr[1] = new IntWritable(11);
		arr[2] = new IntWritable(12);
		intArr.set(arr);
		
		node = new Node();
		node.setNodeID(101);
		node.setNodeType(NodeType.Structure);
		node.setPageRank(0.33f);
		node.setAdjList(intArr);
		
		
	}
	
	/**
	 * Test Node ID
	 */
	@Test
	public void test1(){
		System.out.println("NodeID: " + node.getNodeID());

		assertEquals("NodeID: ", node.getNodeID(), 101);
		
	}
	/**
	 * Test NodeType
	 */
	@Test
	public void test2(){
		System.out.println("NodeType: " + node.getNodeType());

		assertEquals("NodeID: ", node.getNodeType(), NodeType.Structure);
		
	}
	
	
	/**
	 * Test PageRank
	 */
	
	
	@Test
	public void test3(){
		System.out.println("PageRank: " + node.getPageRank());

		assertEquals("PageRank: ", node.getPageRank(), 0.33f,0.01);
		
	}
	/**
	 * Test Elements in adjlist
	 */

  	@Test
	public void test4(){
		IntWritableArray nodeAdjList = node.getAdjList();
		IntWritable[] nodeArray = nodeAdjList.get();
		System.out.println("AdjList: ");
		int k=0;
		for(IntWritable i : arr){
			assertTrue(Arrays.asList(nodeArray).contains(arr[k]));
			System.out.println(arr[k]);
			k++;
		}
	}
  	
  	
  	
  	@Test
	public void test5(){
	/*	IntWritableArray nodeAdjList = node.getAdjList();
		IntWritable[] nodeArray = nodeAdjList.get();
		System.out.println("AdjList: ");
		int k=0;
		for(IntWritable i : arr){
			assertTrue(Arrays.asList(nodeArray).contains(arr[k]));
			System.out.println(arr[k]);
			k++;
		} */
  		System.out.println("Test String!");
  		
  		System.out.println(node.toString());
	}

	
}
