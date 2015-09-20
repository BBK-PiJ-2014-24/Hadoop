/**
 * Reducer that selects the ordered top N from the reduce list 
 */
package chainMaps;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce2
	extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	// Field
	// -----
	
	// create a REVERSE TreeMap
	private TreeMap<IntWritable, Text> rankTree = new TreeMap<IntWritable, Text>(Collections.reverseOrder());
	
	// REDUCER Method
	//---------------	
	    public void reduce(Text key, Iterable<IntWritable> values, Context context
	                       ) throws IOException, InterruptedException {
	      
	      int sum = 0;
	      
	      
	      // sum the 1's
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	    
	      // put (sum, word) into Tree. sum is key for ordering
	      rankTree.put(new IntWritable(sum), new Text(key));
	      if (rankTree.size()>3)
	    		rankTree.remove(rankTree.lastKey());    // push out the lowest count
	
	     
	      
	    }
	    
	    
	    // CLEAN UP
	    // --------
	    // Clean Up happens when all the reduces are in. In effect, this is a 
	    // "combiner" within a reducer. A stand alone combiner may not be called, so
	    // to enforce one, it is built inside. See p44 of "Data-Intensive Text Processing
	    // with MapReduce" Lin & Dyer. 
	    // Also called " in-mapper combining" or 'local aggregation"
	    // Tree is Revered so CORRECT Descending ORDER of TREE
	    
	    @Override
	    public void cleanup(Context context) throws IOException, InterruptedException{
	    	
			for(Map.Entry<IntWritable, Text> entry : rankTree.entrySet()){
	  
	    		context.write(entry.getValue(), entry.getKey());
	    	}
	    }
	    
	    
}
