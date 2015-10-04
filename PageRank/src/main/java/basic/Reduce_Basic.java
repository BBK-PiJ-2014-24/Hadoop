package basic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utilities.Node;
import utilities.NodeType;

public class Reduce_Basic extends Reducer<IntWritable, Node, IntWritable, Node>{
	
	private float sumProbMass = 0f;
	
	public void reduce(IntWritable key, Iterable<Node> values, Context context) throws IOException, InterruptedException{
		
		Node m = new Node();
		m.setNodeID(key.get());
		m.setNodeType(NodeType.CompleteStructure);
		
		
		for(Node n : values ){
			
			if (n.getNodeType().equals(NodeType.AdjList)){
				m.setAdjList(n.getAdjList());
			}
			else{
				sumProbMass += n.getPageRank();
			}	
		}
		
		m.setPageRank(sumProbMass);
		context.write(key, m);	
	}

}
