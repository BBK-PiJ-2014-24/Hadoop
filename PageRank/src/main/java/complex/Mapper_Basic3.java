package chain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utilities.IntWritableArray;
import chain.Node;
import chain.NodeType;

public class Mapper_Basic3 extends Mapper<LongWritable, Text, IntWritable, Node >{
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		
		
		//System.out.println("value: " + value.toString());
		
		// Emit Node with AdjList
		/*Node nextRoundNode = new Node();
		nextRoundNode.setNodeType(NodeType.AdjList);
		int id = (int) node.getNodeID();
		nextRoundNode.setNodeID(id);
		nextRoundNode.setPageRank(0.0f);    // Dummy pageRank
		nextRoundNode.setAdjList(node.getAdjList()); 
		*/
		Node nextRoundNode = Node.create(value.toString());
		nextRoundNode.setNodeType(NodeType.AdjList);
		nextRoundNode.setPageRank(0.0f);    // Dummy pageRank
		System.out.println("MAP OUTPUT\t" + nextRoundNode.toString());
		context.write(new IntWritable(nextRoundNode.getNodeID()), nextRoundNode);
		
		
		// Distribute Node's Prob Mass (if Node has a adjList)
		Node node = Node.create(value.toString());
		if(node.getAdjList().length > 0){
			float probMass = node.getPageRank()/node.getAdjList().length;
			//System.out.println("PAGE RANK\t" + node.getNodeID() + "\t" +  node.getPageRank()+"\t"+ node.getAdjList().length+"\t"+probMass );
			int[] adjArr = node.getAdjList();
			for(Integer i : adjArr){
				Node n = new Node();
				n.setNodeType(NodeType.ProbMass);
				n.setNodeID(i);
				n.setPageRank(probMass);
			    System.out.println("MAP OUTPUT\t"+n.toString());
				context.write(new IntWritable(n.getNodeID()), n);
			}
		}	
	}
	

}
