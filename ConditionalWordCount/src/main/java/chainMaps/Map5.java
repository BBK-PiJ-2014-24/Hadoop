// map that pushes the Top N results into a tree. 

package chainMaps;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Map5 extends Mapper<Text, IntWritable, NullWritable, Text > 
{
	
	private TreeMap<Integer,Text> rankTree = new TreeMap<Integer, Text>();
	
	public void map(Iterable<Text> key,  Iterable<IntWritable> value, Context context) 
			throws IOException, InterruptedException
	{
	
		  // Break File into subTokens
		//  StringTokenizer itr = new StringTokenizer(value.toString());
		  
		  
		  
		  for (IntWritable v : value) {
		     System.out.println(v);  
			  //sum += val.get();
		      }
		  
		 // while(itr.hasMoreTokens()){
			  
			  // Token 1: word
		//	  String word = itr.nextToken();
		//	  System.out.println("word =" + word);
			  // Token 2: the word count
			  //if(!itr.hasMoreTokens()) break;
			  //String count = itr.nextToken();
			  //Integer count = Integer.parseInt(itr.nextToken());
			  //System.out.println(count);
			  // The value to be placed in the tree
			  //Text v = new Text(word + "\t" + count);
			  //System.out.println(v.toString());
			  // key = count, value = word + word count
			  //rankTree.put(count, v);
			
			  
			// purge the tree of lowest count word  
		/**	if(rankTree.size() > 3){
				rankTree.remove(rankTree.firstKey());
			}
			
			for(Integer i : rankTree.keySet()){
				System.out.println(i + rankTree.values().toString());
			}
		**/
		//}	
	}
	
	// Clean Up method called automatically by Hadoop. Sweeps all 
	// Maps into one Table? 
	
/**	protected void cleanUp(Context context) throws IOException, InterruptedException
	{
		for(Text t : rankTree.values()){
			context.write(NullWritable.get(), t);
		}
	}
	**/
}
