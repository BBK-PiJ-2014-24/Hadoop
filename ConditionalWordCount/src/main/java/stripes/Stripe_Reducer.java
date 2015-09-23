/**
 * Reducer for Absolute Count (Not Relative Frequency)
 */
package stripes;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utilities.MyMapWritable;
import utilities.MyMapWritable2;

public class Stripe_Reducer extends Reducer<Text, MyMapWritable2, Text, MyMapWritable2>{

	
	
	private MyMapWritable2 result = new MyMapWritable2();

	
	public void reduce(Text key, Iterable<MyMapWritable2> value, Context context) throws IOException, InterruptedException{
		
	result.clear();
		
		for(MyMapWritable2 val : value){
			sumAll(val);
		}
		
			//Text t = new Text(result.toString());
			//System.out.println("\nHELLO\n");
			//System.out.println(result.toString());
			context.write(key, result); //result;	
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
		}
	}
}
