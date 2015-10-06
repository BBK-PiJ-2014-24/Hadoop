package reader;

import java.io.IOException;
import java.util.ArrayList;
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

	private int totalCountOfEdges = 0;
	List<Node> nodeList = new ArrayList<Node>();
	
	public void Reduce(IntWritable key, Iterable<IntWritable> values, Context context){
		
		Node n = new Node();
		n.setNodeType(NodeType.CompleteStructure);
		n.setNodeID(key.get());
		
		Iterator<IntWritable> it = values.iterator();  
		if(it.hasNext()){					// check if node has any edges
		
			int size=0;
			for(IntWritable i : values){   // find number of edges first
				size++;
			}
			
			int[] adjList = new int[size];  // instantiate int[]
			
			int k=0;
			for(IntWritable i : values){  // populate
				adjList[k] = i.get();
				k++;
			}
			totalCountOfEdges += size;
			n.setAdjList(adjList);  // set to Node
			nodeList.add(n);   // add to Node List
		}
	}
		
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
			
		Float pageRank = (float) (1/totalCountOfEdges);
		for(Node node : nodeList){
			node.setPageRank(pageRank);
			context.write(new IntWritable(node.getNodeID()), node);
		}
			
	}
}

