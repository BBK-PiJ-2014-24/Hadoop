/**
 * Map2 - Screens Out Punctuation.
 */
package chainMaps;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map2 extends Mapper<Object, Text, Text, Text>{
	
	// private final static NullWritable n = NullWritable.get();
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        word.set(ParseUtilities.screenPunctuation(word)); //check punctuation
       
        context.write(word, word);
      }
    }

}
