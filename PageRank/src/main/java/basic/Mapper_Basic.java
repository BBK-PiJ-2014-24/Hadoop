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
		Node structNode = new Node();
		structNode.setNodeType(NodeType.Structure);
		structNode.setNodeID(nodeID.get());
		structNode.setPageRank(node.getPageRank());
		structNode.setAdjList(node.getAdjList());
		context.write(nodeID, structNode);
		
		
		// Distribute Node's Prob Mass
		
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
