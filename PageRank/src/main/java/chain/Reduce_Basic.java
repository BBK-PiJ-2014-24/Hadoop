package chain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import chain.Node;
import chain.NodeType;

public class Reduce_Basic extends Reducer<IntWritable, Node, IntWritable, Node>{
	
	
	
	public void reduce(IntWritable key, Iterable<Node> values, Context context) throws IOException, InterruptedException{
		
		float sumProbMass = 0f;
		Node m = new Node();
		m.setNodeID(key.get());
		m.setNodeType(NodeType.CompleteStructure);
		
		for(Node n : values ){
			//Node n = Node.create(t.toString());
			if (n.getNodeType().equals(NodeType.AdjList)){
				m.setAdjList(n.getAdjList());
			}
			else{
				sumProbMass += n.getPageRank();
			}	
		}
		// System.out.println("AdjList: " + m.getAdjList().toString());
		m.setPageRank(sumProbMass);
		System.out.println("REDUCE OUTPUT " + m.toString());
		context.write(key, m);	
	}

}
