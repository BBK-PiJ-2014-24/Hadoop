/**
 * Reducer for topN
 */

package chain;

import java.io.IOException;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class topN_Reducer extends Reducer<IntWritable, Node, IntWritable, Node> {
	
	private TreeMap<Float, Node> rankTree = new TreeMap<Float, Node>(Collections.reverseOrder());
	
	
	public void reduce(IntWritable key, Iterable<Node> values, Context context) {
	
		for(Node n : values){
			rankTree.put(n.getPageRank(),n);
			if(rankTree.size()>3){
				rankTree.remove(rankTree.lastKey());
			}
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		for(Float f : rankTree.keySet()){
			Node n = rankTree.get(f);
			context.write(new IntWritable(n.getNodeID()), n );
		}
	}
	
	
			
}
