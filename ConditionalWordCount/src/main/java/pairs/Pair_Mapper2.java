/**
 * Mapper for Calc Cond Prob with the Pairs Method
 */


package pairs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utilities.ParseUtilities;

public class Pair_Mapper2 extends Mapper<Object,Text,Text,IntWritable>{

	
	// Fields
	// ------
	private IntWritable ONE = new IntWritable(1);
	
	
	
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
	    		Text wordPair = new Text(word + " " + coWord);
	    		context.write(wordPair, ONE);
	    	}   		   	
	    }
	}	
}
