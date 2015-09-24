/**
 * Stripe_Reducer that calculates the relative frequency.
 */
		

package stripes;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import utilities.MyMapWritable2;

public class Stripe_Reducer2 extends Reducer<Text, MyMapWritable2, Text, MyMapWritable2>{

	// fields
	// ------
	
	private MyMapWritable2 result = new MyMapWritable2();
	private IntWritable marginalCount = new IntWritable(0);
	
	// Reduce
	// ------
	public void reduce(Text key, Iterable<MyMapWritable2> value, Context context) throws IOException, InterruptedException{
		
	result.clear();
		
		for(MyMapWritable2 val : value){
			sumAll(val);
		}
		
			//Text t = new Text(result.toString());
			//System.out.println("\nHELLO\n");
			//System.out.println(result.toString());
			//context.write(key, result); //result;	
	}
	
	
	private void sumAll(MyMapWritable2 mw){
		Set<Writable> keys = mw.keySet();  // get the set of keys so u can iterate MapW
										   // in actual fact, there is only 1 key in Set = for
		
		for(Writable key : keys){
		
			IntWritable moreCount = (IntWritable) mw.get(key); // get the new count update
			
			if(result.containsKey(key)){   //if results map already has key
				IntWritable count = (IntWritable)result.get(key); // get exist count
				count.set(count.get() + moreCount.get());  // combine the two together.
			}
			else{
				result.put(key, moreCount);
			}
			marginalCount.set(marginalCount.get() + moreCount.get());
		}  // end for
	}  // end sumAll
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		Set<Writable> keys = result.keySet();
		
		
		for(Writable key : keys){
			IntWritable finalCount = (IntWritable) result.get(key);
			DoubleWritable freq = new DoubleWritable(500); // must inside loop so new freq for each line.
			Double x =  (finalCount.get() *1.0)/marginalCount.get();
			freq.set(x);
			//System.out.println("******* " + finalCount + "/" + marginalCount  + "=" + freq);
			//System.out.println("++++++++ " + key + " " + freq);
			result.put(key, freq);
			// System.out.println("++++++++ " + result.toString());
		}
		context.write(new Text("for"), result);
	}
	
}

