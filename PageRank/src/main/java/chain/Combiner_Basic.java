/**
Combiner sums partial Partial PageRank and also emits node structure to 
*/

package chain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Combiner_Basic extends Reducer<IntWritable, Node, IntWritable, Node>{
	
	
	
	public void reduce(IntWritable key, Iterable<Node> values, Context context) throws IOException, InterruptedException{
		
		float sumProbMass = 0f;
		Node m = new Node();
		m.setNodeID(key.get());
		
		
		for(Node n : values ){
			//Node n = Node.create(t.toString());
			if (n.getNodeType().equals(NodeType.AdjList)){
				System.out.println("COMBINER OUTPUT " + n.toString());
				context.write(key, n);
			}
			else{
				m.setNodeType(NodeType.ProbMass);
				sumProbMass += n.getPageRank();
			}	
		}
		// System.out.println("AdjList: " + m.getAdjList().toString());

			m.setPageRank(sumProbMass);
	
		System.out.println("COMBINER OUTPUT " + m.toString());
		context.write(key, m);	
	}

}
