/**
 * Mean_Mapper is a Mapper for calc mean, using the compound value as a technique
 */

package mean;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.yarn.*;


public class MeanMapper 
	extends Mapper<Object, Text, Text, MyCompoundValue>{
	
	// Fields
	// ------
 
    private MyCompoundValue cv = new MyCompoundValue();
  

    // Map
    // ---
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
      StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
      
      while (itr.hasMoreTokens()) {
        String name = itr.nextToken(); 
        int score= Integer.parseInt(itr.nextToken());
        
        cv.setTotal(score);
        cv.setCount(1);		
 
        context.write(new Text(name), cv);
      }
    }
    
}