/**
 * Reducer for topN
 */

package chain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;





public class topN_Reducer2 extends Reducer<IntWritable, Node, IntWritable, Node> {
	
	//private TreeMap<Float, Node> rankTree = new TreeMap<Float, Node>(Collections.reverseOrder());
	private ArrayList<ItemPair> list = new ArrayList<ItemPair>();
	
	public void reduce(IntWritable key, Iterable<Node> values, Context context) {
	
		for(Node n : values){
			Node newNode = Node.create(n.toString());
			list.add(new ItemPair(newNode.getPageRank(),newNode));
		}	
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		Collections.sort(list, Collections.reverseOrder()); // Sort the list in desc order
		
		for(int i=0; i<10; i++){
			ItemPair ip = list.get(i);
			Node n = ip.getNode();
			Node newNode = Node.create(n.toString());
			System.out.println("topN REDUCER: " + ip.getPageRank() +"\t"+ newNode.toString());
			context.write(new IntWritable(newNode.getNodeID()), newNode );
		}
	}			
}



/** Container class for (pageRank, Node) Pair  that which will be added to 
 * the topN List.
 * @author hduser
 *
 */
class ItemPair implements Comparable<ItemPair>{
	
	private float p;  // pageRank
	private Node n;  // coWord
	
	public ItemPair(float p, Node n){
		this.p = p;
		this.n = n;
	}

	public double getPageRank(){
		return p;
	}
		
	public Node getNode(){
		return n;
	}
	public int compareTo(ItemPair other) {
		if(this.p > other.p) return 1;
		if(this.p == other.p) return 0;
		return -1;
	}
	
	public String toString(){
		return n + "\t" + p;
	}	
}
