/**
 * Reducer That Calcs the TopN of Conditional Probs
 * 
 * mw - Map of 
 */

package stripes;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
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

public class Stripe_Reducer4 extends Reducer<Text, MyMapWritable2, Text, DoubleWritable>{

	// fields
	// ------
	
	private MyMapWritable2 result = new MyMapWritable2();
	private MyMapWritable2 topResult = new MyMapWritable2();
	private IntWritable marginalCount = new IntWritable(0);
	// private TreeMap<Double, Text> rankTree = new TreeMap<Double, Text>(Collections.reverseOrder());
	private ArrayList<ItemStripe> list = new ArrayList<ItemStripe>();
	
	// Reduce
	// ------
	public void reduce(Text key, Iterable<MyMapWritable2> value, Context context) throws IOException, InterruptedException{
		
	result.clear();
		
	// Sum Stripes
	// -----------
		for(MyMapWritable2 stripe : value){
			sumAll(stripe); // stripe (co-words, counts) Amends Field Variable, result
		}
	// Add to Top 10 List
	// ------------------	
		Set<Writable> sumKeys = result.keySet();
		for(Writable sumKey : sumKeys){
			Text coWord = (Text) sumKey;  // co-word = key 
			double x  = ((IntWritable)(result.get(sumKey))).get(); // count = value
			list.add(new ItemStripe(x, coWord));	
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
				//System.out.println("\nsumAll - before Update: " + key.toString() + "\t"   + count.get() + "\tIncrease Count: " + moreCount.get() );
				count.set(count.get() + moreCount.get());  // combine the two together.
				//System.out.println("sumAll - after Update: " + key.toString() + "\t"   + count.get());
			}
			else{
				result.put(key, moreCount);
				//System.out.println("\nsumAll - new UPdate: " + key.toString() + "\t"   + moreCount.get());
			}
			//System.out.println("sumAll - MARGINAL BEfore: " + marginalCount.get() + "\tincrease: " + moreCount.get() );
			marginalCount.set(marginalCount.get() + moreCount.get());
			//System.out.println("sumAll - MARGINAL After: " + marginalCount.get());
		}  // end for
	}  // end sumAll
	
	
	
	// Final Reduce to Calc Cond Prob of Top 10
	// ----------------------------------------
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		Collections.sort(list, Collections.reverseOrder()); // Sort the list in desc order
		
		Set<Writable> keys = result.keySet();
		//MyMapWritable2 prob = new MyMapWritable2();
		
		
		for(int i=0;i<10;i++){
			ItemStripe ip = list.get(i);
			double f = ip.getFreq();
			DoubleWritable freq = new DoubleWritable(500); // must inside loop so new freq for each line.
			double x =  f /marginalCount.get();
			x = round(x,4);
			freq.set(x);
			Text coWord = ip.getText();
			context.write(coWord, freq);
		}
	}
	
	private double round(double d, int dp){	
		BigDecimal bd = new BigDecimal(d);
		bd = bd.setScale(dp, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
}





class ItemStripe implements Comparable<ItemStripe>{
	
	private double x;  // frequency
	private Text t;  // coWord
	
	public ItemStripe(double x, Text t){
		this.x = x;
		this.t = t;
	}

	public double getFreq(){
		return x;
	}
		
	public Text getText(){
		return t;
	}
	
	public int compareTo(ItemStripe other) {
		if(this.x > other.x) return 1;
		if(this.x == other.x) return 0;
		return -1;
	}
	
	public String toString(){
		return t.toString() + "\t" + x;
	}	
}