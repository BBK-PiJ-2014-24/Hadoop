/**
 * Map That Does "in_Mapping". That is, does a quick sum within the Map before
 * pumping out.
 * 
 *So emit (word, 6) rather than (word, 1) x6
 */

package inMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utilities.ParseUtilities;

public class inMapMapper extends Mapper<Object, Text, Text, IntWritable>{
	
    
	private Map<String,Integer> subMap = new HashMap<String, Integer>();
    private Text word = new Text();
    private IntWritable count = new IntWritable();

    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
        
    	String str = itr.nextToken();  
    	str = ParseUtilities.screenPunctuation(str);
    	
    	if(subMap.containsKey(str)){
    		subMap.put(str, subMap.get(str) + 1);
    	}
    	else{
    		subMap.put(str, 1);
    	}
      }
    	
      
      for(Map.Entry<String,Integer> entry : subMap.entrySet()){
    		word.set(entry.getKey());
    		count.set(entry.getValue());
    		context.write(word, count);
    	}
     
    }
}
