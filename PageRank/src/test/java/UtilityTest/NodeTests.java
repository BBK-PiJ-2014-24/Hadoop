/**
 * Test the Node Class
 */

package UtilityTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import utilities.IntWritableArray;
import utilities.IntWritableArray2;
import utilities.Node;
import utilities.NodeType;

public class NodeTests {


	private Node node;
	private int[] intArr;
	private ArrayPrimitiveWritable writArr;
	
	
	@Before
	public void setUp(){
		intArr = new int[]{10,11,12};
	
		
		node = new Node();
		node.setNodeID(101);
		node.setNodeType(NodeType.CompleteStructure);
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

		assertEquals("NodeID: ", node.getNodeType(), NodeType.CompleteStructure);
		
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
		int[] nodeAdjList = node.getAdjList();
		System.out.println("AdjList: ");
		Assert.assertArrayEquals("Test AdjList Array", intArr, nodeAdjList);
	
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
  		System.out.println("\nTest String!");
  		
  		System.out.println(node.toString());
	}

  	
  	/**
  	 * Test adding to AdjList
  	 */
  	
  	@Test
  	public void test6(){
  		
  		int[] x = new int[]{1,2,3}; 
  		int [] ans = new int []{10,11,12,1,2,3};
  		node.setAdjList(x);
  		Assert.assertArrayEquals(ans,node.getAdjList());
  	}
  	
  	
	
}
