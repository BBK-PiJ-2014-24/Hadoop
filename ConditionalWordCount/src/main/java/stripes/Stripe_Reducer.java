package stripes;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Stripe_Reducer extends Reducer<Text, MapWritable, Text, MapWritable>{

	
	
	private MapWritable result = new MapWritable();
	
	public void reduce(Text key, Iterable<MapWritable> value, Context context) throws IOException, InterruptedException{
		
	result.clear();
		
		for(MapWritable val : value){
			sumAll(val);
		}
		context.write(key, result);	
	}
	
	
	private void sumAll(MapWritable mw){
		Set<Writable> keys = mw.keySet();  // get the set of keys so u can iterate MapW
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
