package reader;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import utilities.Node;
import utilities.NodeType;

public class Reduce_Reader extends Reducer<IntWritable, IntWritable, IntWritable, Node>{

	private static int totalCountOfNodes = 0;
	List<Node> nodeList = new ArrayList<Node>();
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
		System.out.println("HELLO in");
		Node n = new Node();
		n.setNodeType(NodeType.CompleteStructure);
		n.setNodeID(key.get());
		
		
		int size=0;
		ArrayList<IntWritable> cacheList = new ArrayList<IntWritable>();
		for(IntWritable i : values){   // find number of edges first
			size++;
			IntWritable element = new IntWritable(i.get());
			cacheList.add(element);
			//System.out.println(i.get());
		}
	
		System.out.println("cacheList = " + cacheList.toString());
		
		int[] adjList = new int[size];  // instantiate int[]
		int k=0;
		for(IntWritable i : cacheList){
			//System.out.println(i.get());
			adjList[k] = i.get();
			k++;
		}

		
		totalCountOfNodes += 1;
		n.setAdjList(adjList);  // set to Node
		nodeList.add(n);   // add to Node List
		
		//context.write(new IntWritable(n.getNodeID()), n);
		
	}
		
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		
		Float pageRank = (float) (1.0/totalCountOfNodes);
		for(Node node : nodeList){
			node.setPageRank(pageRank);
			context.write(new IntWritable(node.getNodeID()), node);
			System.out.println(node.toString());
		}
		
		System.out.println("Total Edges = " + totalCountOfNodes);
			
	} 	
}

