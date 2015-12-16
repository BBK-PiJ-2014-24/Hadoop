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

public class Pair_Mapper3 extends Mapper<Object,Text,WordPair,IntWritable>{

	
	// Fields
	// ------
	private IntWritable ONE = new IntWritable(1);
	private WordPair wordPair= new WordPair();
	
	// Map
	// ---
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		int marginalCount = 0;
		String targetWord = "for";
	      
	    while (itr.hasMoreTokens()){
	    	String word = itr.nextToken();
	    	word = word.toLowerCase();
	    	word = ParseUtilities.screenPunctuation(word);
	    	word = ParseUtilities.screenStem(word);
	    	if(word.equals(targetWord) && itr.hasMoreTokens()){
	    		String coWord = itr.nextToken();
	    		System.out.println("MAPPER3 START coWord: " + coWord);
	    		String screenCoWord = ParseUtilities.screenStem(coWord);
	    		screenCoWord = ParseUtilities.screenPunctuation(screenCoWord);
	    		screenCoWord = screenCoWord.toLowerCase();
	    		System.out.println("MAPPER3 coWord: " + screenCoWord);
	    		wordPair.set(word, screenCoWord);
	    		context.write(wordPair, ONE);
	    		marginalCount++;
	    		wordPair.set(targetWord, "*");
		    	//System.out.println("MARGINAL *");
		    	context.write(wordPair, new IntWritable(marginalCount)); // Send info for marginal count 
	    	}   		   	
	    }
	}
}
