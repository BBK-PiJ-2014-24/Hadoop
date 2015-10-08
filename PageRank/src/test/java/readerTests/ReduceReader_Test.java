/**
 * Create 2 lists of input for node 0 and node 4. Tests that reducer creates nodes 0 4 
 * from data.
 */


package readerTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import reader.Reduce_Reader;
import utilities.Node;
import utilities.NodeType;

public class ReduceReader_Test {

		// Fields
		// ------
		
		ReduceDriver<IntWritable, IntWritable, IntWritable, Node> reduceDriver;
		Reduce_Reader myReducer;
		Node n0;
		Node n4;
	
		int[] a0;
		int[] a4;		
		
		List<IntWritable> list0;
		List<IntWritable> list4;
		
		@Before
		public void setUp(){
			reduceDriver = new ReduceDriver<IntWritable, IntWritable, IntWritable, Node>();
			myReducer = new Reduce_Reader();
			reduceDriver.setReducer(myReducer);
		
			
			// input for nodeid 0 and 4
			// ------------------------
			
			list0 = new ArrayList<IntWritable>();
				list0.add(new IntWritable(2));
				list0.add(new IntWritable(139));
				list0.add(new IntWritable(140));
				list0.add(new IntWritable(141));
				list0.add(new IntWritable(142));
				list0.add(new IntWritable(799));
			
			list4 = new ArrayList<IntWritable>();
				list4.add(new IntWritable(1));
				list4.add(new IntWritable(10));
				list4.add(new IntWritable(12));
				list4.add(new IntWritable(18));
				list4.add(new IntWritable(22));
			
				
			// Output Created Nodes 0 and 4 
			// ----------------------------
				
				a0 = new int[]{2, 139, 140, 141, 142, 799};
				a4 = new int[]{1, 10, 12, 18, 22};
				
				n0 = new Node();
				n0.setNodeType(NodeType.CompleteStructure);
				n0.setNodeID(0);
				n0.setPageRank(0.90909f);
				n0.setAdjList(a0);
				
				n4 = new Node();
				n4.setNodeType(NodeType.CompleteStructure);
				n4.setNodeID(4);
				n4.setPageRank(0.90909f);
				n4.setAdjList(a4);	
				
				
		}
		
		@Test
		public void test1() throws IOException{
			
			reduceDriver.withInput(new IntWritable(0), list0);
			reduceDriver.withInput(new IntWritable(4), list4);
			reduceDriver.withOutput(new IntWritable(0), n0);
			reduceDriver.withOutput(new IntWritable(4), n4);
			
			reduceDriver.runTest();
		}

	
}
