/**
 * Map1 - Turns everything to LowerCase
 */


package chainMaps;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.yarn.*;


public class Map1 
extends Mapper<Object, Text, Text, Text>{
	
    //private final static NullWritable n = NullWritable.get();
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        String str = word.toString();   // convert text to string
        str = str.toLowerCase();
        word.set(str);      // reset word to lower case
        context.write(word, word);
      }
    
    }
}
