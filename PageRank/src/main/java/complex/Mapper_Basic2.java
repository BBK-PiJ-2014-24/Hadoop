package chain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import utilities.IntWritableArray;
import utilities.Node;
import utilities.NodeType;

public class Mapper_Basic2 extends Mapper<IntWritable, Node, IntWritable, Node >{
	
	
	@Override
	public void map(IntWritable nodeID, Node node, Context context) throws IOException, InterruptedException{
		
		
		
		// Emit Node with AdjList
		Node nextRoundNode = new Node();
		nextRoundNode.setNodeType(NodeType.AdjList);
		int id = (int) node.getNodeID();
		nextRoundNode.setNodeID(id);
		nextRoundNode.setAdjList(node.getAdjList());
		context.write(new IntWritable(id), nextRoundNode);
		
		
		// Distribute Node's Prob Mass (if Node has a adjList)
		if(node.getAdjList().length > 0){
			float probMass = node.getPageRank()/node.getAdjList().length;
			int[] adjArr = node.getAdjList();
			for(Integer i : adjArr){
				Node n = new Node();
				n.setNodeType(NodeType.ProbMass);
				n.setNodeID(i);
				n.setPageRank(probMass);
			
				context.write(new IntWritable(n.getNodeID()),n);
			}
		}	
	}
	

}
