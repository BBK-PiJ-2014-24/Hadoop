package mean;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MeanReducer 
	extends Reducer<Text, MyCompoundValue,Text,DoubleWritable> {
	    
	    
		private DoubleWritable result = new DoubleWritable();

	    public void reduce(Text key, Iterable<MyCompoundValue> values, Context context
	                       ) throws IOException, InterruptedException {
	      
	    	
	      int sum = 0;
	      int counter = 0;
	      
	      for (MyCompoundValue cv : values) {
	        sum += cv.getTotal();
	        counter += cv.getCount();
	      }
	      
	      double ans = sum/counter;
	      result.set(ans);
	      
	      context.write(key, result);
	    }
	
}
