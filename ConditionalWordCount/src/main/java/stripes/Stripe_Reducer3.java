/**
 * Reducer That Calcs the TopN of Conditional Probs
 * 
 * mw - Map of 
 */

package stripes;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import utilities.MyMapWritable2;

public class Stripe_Reducer3 extends Reducer<Text, MyMapWritable2, Text, DoubleWritable>{

	// fields
	// ------
	
	private MyMapWritable2 result = new MyMapWritable2();
	private MyMapWritable2 topResult = new MyMapWritable2();
	private IntWritable marginalCount = new IntWritable(0);
	private TreeMap<Integer, Text> rankTree = new TreeMap<Integer, Text>(Collections.reverseOrder());
	
	
	// Reduce
	// ------
	public void reduce(Text key, Iterable<MyMapWritable2> value, Context context) throws IOException, InterruptedException{
		
	result.clear();
		
	// Sum Stripes
	// -----------
		for(MyMapWritable2 stripe : value){
			sumAll(stripe); // stripe (co-words, counts) Amends Field Variable, result
		}
	// Select only Top 3 Scores
	// ------------------------	
		Set<Writable> sumKeys = result.keySet();
		for(Writable sumKey : sumKeys){
			Text coWord = (Text) sumKey;  // co-word = key 
			int x  = ((IntWritable)(result.get(sumKey))).get(); // count = value
			
			rankTree.put(x, coWord);
			if (rankTree.size()>3)
	    		rankTree.remove(rankTree.lastKey());    // push out the lowest count
		}	
	}
	
	// MEthod that Sums Stripes from diff MAps
	// ---------------------------------------
	private void sumAll(MyMapWritable2 stripe){
		Set<Writable> keys = stripe.keySet();  // get the set of keys (of co-words) 
										       // so u can iterate stripe
		
		for(Writable key : keys){  // for each co-word
		
			IntWritable moreCount = (IntWritable) stripe.get(key); // get the new count update
			
			if(result.containsKey(key)){   //if results map already has co-word in key
				IntWritable count = (IntWritable)result.get(key); // get exist count
				count.set(count.get() + moreCount.get());  // combine the two together.
			}
			else{
				result.put(key, moreCount);
			}
			marginalCount.set(marginalCount.get() + moreCount.get());
		}  // end for
	}  // end sumAll
	
	
	
	// Final Reduce to Calc Cond Prob of Top 3
	// ----------------------------------------
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		Set<Writable> keys = result.keySet();
		//MyMapWritable2 prob = new MyMapWritable2();
		
		
		for(Integer i : rankTree.keySet()){
			DoubleWritable freq = new DoubleWritable(500); // must inside loop so new freq for each line.
			double x =  (i *1.0)/marginalCount.get();
			x = round(x,4);
			freq.set(x);
			Text coWord = rankTree.get(i);
			context.write(coWord, freq);
		}
	}
	
	private double round(double d, int dp){
		
		BigDecimal bd = new BigDecimal(d);
		bd = bd.setScale(dp, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
	
	
	
}