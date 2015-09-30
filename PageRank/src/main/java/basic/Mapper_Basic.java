package basic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import utilities.IntWritableArray;
import utilities.Node;
import utilities.NodeType;

public class Mapper_Basic extends Mapper<IntWritable, Node, IntWritable, Node >{
	
	
	@Override
	public void map(IntWritable nodeID, Node node, Context context) throws IOException, InterruptedException{
		
		
		
		// Emit Node with NodeStructure
		Node transitNode = new Node();
		transitNode.setNodeType(NodeType.Structure);
		transitNode.setNodeID(nodeID.get());
		transitNode.setPageRank(node.getPageRank());
		transitNode.setAdjList(node.getAdjList());
		context.write(nodeID, transitNode);
		
		
		// Distribute Node's Prob Mass
		
		float probMass = node.getPageRank()/node.getAdjList().length();
		int[] adjArr = node.getAdjList().get();
		for(Integer i : adjArr){
			Node n = new Node();
			n.setNodeType(NodeType.ProbMass);
			n.setNodeID(i);
			n.setPageRank(probMass);
			
			context.write(n.getNodeID(),n);
		}	
	}
	

}
