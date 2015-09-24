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
import utilities.MyCompoundValue;

import utilities.ParseUtilities;
import utilities.WordPair;

public class Pair_Mapper2 extends Mapper<Object,Text,WordPair,IntWritable>{

	
	// Fields
	// ------
	private IntWritable ONE = new IntWritable(1);
	private WordPair wordPair= new WordPair();
	
	// Map
	// ---
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		int marginalCount = 0;
	      
	    while (itr.hasMoreTokens()){
	    	String word = itr.nextToken();
	    	word = word.toLowerCase();
	    	word = ParseUtilities.screenPunctuation(word);
	    	if(word.equals("for") && itr.hasMoreTokens()){
	    		String coWord = itr.nextToken();
	    		coWord = coWord.toLowerCase();
	    		coWord = ParseUtilities.screenPunctuation(coWord);
	    		wordPair.set(word, coWord);
	    		context.write(wordPair, ONE);
	    		marginalCount++;
	    	}   		   	
	    }
	    wordPair.set("for", "*");
	    context.write(wordPair, new IntWritable(marginalCount)); // Send info for marginal count 
	}	

	
}
