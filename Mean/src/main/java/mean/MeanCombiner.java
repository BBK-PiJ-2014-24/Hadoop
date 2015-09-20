/**
 * The Combiner extends Reducer. However it's:
 * input key/value = Mapper output key/value
 * output key/value = Mapper output key/value = Reducer = input/value.
 * 
 * This means the combiner needs to adjust the output of the reducer code so that it's 
 * output is exactly the same as the input.
 * 
 * This is because the combiner may or may not be called in the MR.
 */


package mean;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MeanCombiner extends Reducer<Text, MyCompoundValue,Text,MyCompoundValue> {
    
	private MyCompoundValue cv = new MyCompoundValue();

    public void reduce(Text key, Iterable<MyCompoundValue> values, Context context
                       ) throws IOException, InterruptedException {
      
    	
      int sum = 0;
      int counter = 0;
      
      for (MyCompoundValue cv : values) {
        sum += cv.getTotal();
        counter += cv.getCount();
      }
      
      cv.setTotal(sum);
      cv.setCount(counter);
      
      context.write(key, cv);
    }
}
