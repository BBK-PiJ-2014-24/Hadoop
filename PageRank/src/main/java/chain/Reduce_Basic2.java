/**
 * Reducer with topN
 */
package chain;

import java.io.IOException;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import chain.Node;
import chain.NodeType;

public class Reduce_Basic2 extends Reducer<IntWritable, Node, IntWritable, Node>{
	
	private TreeMap<Float, Node> rankTree = new TreeMap<Float, Node>(Collections.reverseOrder());
	
	
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
		//context.write(key, m);
		rankTree.put(m.getPageRank(), m);
		if(rankTree.size()>3)
			rankTree.remove(rankTree.lastKey());
	}

	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		for(Float f : rankTree.keySet()){
			
			Node n = rankTree.get(f);
			context.write(new IntWritable(n.getNodeID()), n );
		}
	}
}
