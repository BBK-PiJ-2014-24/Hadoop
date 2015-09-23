/**
 * Stripe Mapper. Output Value is MyMapWritable as need toString() for testing.
 */

package stripes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utilities.MyCompoundValue;
import utilities.MyMapWritable;
import utilities.MyMapWritable2;
import utilities.ParseUtilities;

public class Stripe_Mapper extends Mapper<Object, Text, Text, MyMapWritable2>{


	//Fields
	// ------
	
	 private final static IntWritable ONE = new IntWritable(1);
	 private MyMapWritable2 mw = new MyMapWritable2();  // The type is always Writable


	// Map
	// ---
			
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			
			StringTokenizer itr = new StringTokenizer(value.toString());
		      
		    while (itr.hasMoreTokens()){
		    	String word = itr.nextToken();
		    	word = word.toLowerCase();
		    	word = ParseUtilities.screenPunctuation(word);
		    	if(word.equals("for") && itr.hasMoreTokens()){
		    		String coWord = itr.nextToken();
		    		coWord = coWord.toLowerCase();
		    		coWord = ParseUtilities.screenPunctuation(coWord);
		    		Text w = new Text(coWord);
		    		
		    		if(mw.containsKey(w)){
		    			IntWritable count = (IntWritable)mw.get(w);  // Convert to int before
		    			mw.put(w, new IntWritable(count.get()+ 1));    // updating mapWritable
		    		}
		    		else
		    			mw.put(w,ONE);
		    	}	// end if		    	   		   	
		    } // end while
		    context.write(new Text("for"), mw);
		}	
	
	

}
