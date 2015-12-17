/**
 * Reduce for Cond Prob + topN
 */

package pairs;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import utilities.WordPair;

public class Pair_Reducer4 extends Reducer<WordPair, IntWritable, Text, DoubleWritable> {

	private Text star = new Text("*");
	//private IntWritable result = new IntWritable();
	private IntWritable marginalCount = new IntWritable();
	//private TreeMap<Double, String> rankTree = new TreeMap<Double, String>(Collections.reverseOrder());
	private ArrayList<ItemPair> list = new ArrayList<ItemPair>();
	
	// Reduce
	// ------
	/**
	 *  key is ordered with (for,*) on top, because of the comparable
	 */
	public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		
		// find the (for,*) first to calc marginalCount FIRST 
		if(key.getRight().equals(star)){
			//System.out.println("\n**** before marginal count: " + marginalCount.get());
			marginalCount.set(marginalCount.get() + calcSum(values));
			//System.out.println("**** after marginal count" + marginalCount.get() + "\tincrement: " + calcSum(values));
		}
		else{
			int count = calcSum(values);  // calc Total Count
			double freq = (count*1.0)/marginalCount.get();  // calc conditional prob
			freq = round(freq,4);
			String wordPair = key.toString();
			list.add(new ItemPair(freq, wordPair));
			System.out.println("******" + wordPair + ", " + freq);
			//System.out.println("++++ Size of Tree: " + rankTree.size());
		}
	}
	
	
	// calcSum()
	// ---------
	/**
	 * Method to calc any key's Partial sums by iterating its values
	 * @param values
	 * @return
	 */
	// -------------------------------------------------------------
	public int calcSum(Iterable<IntWritable> values){
		int marg = 0;
		for(IntWritable i : values){
			marg += i.get();
		}
		return marg;
	}
	
	
	// cleanup()
	// ---------
	/**
	 * writes only the topN
	 */
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		//System.out.println("++++++++++SORTED+++++++++");
		Collections.sort(list, Collections.reverseOrder()); // Sort the list in desc order
		for(ItemPair i : list){
			System.out.println(i);
		}
		
		for(int i=0; i<10; i++){
			ItemPair ip = list.get(i);
			double f = ip.getFreq();
			DoubleWritable freq = new DoubleWritable(); // must inside loop so new freq for each line.
			freq.set(f);    // convert freq to DoubleWritable
			Text coWord = new Text(ip.getWord()); // convert coWord to WordPair
			//System.out.println("++++++" + coWord.toString() + ", " + freq.get());
			context.write(coWord, freq);
		}
	}
	
	//round()
	// ------
	/**
	 * Rounds a double
	 * @param d - double 
	 * @param dp = dec places
	 * @return
	 */
	private double round(double d, int dp){
		
		BigDecimal bd = new BigDecimal(d);
		bd = bd.setScale(dp, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
		
}	

/** Container class for coword and its frequency that which will be added to 
 * the topN List.
 * @author hduser
 *
 */
class ItemPair implements Comparable<ItemPair>{
	
	private double x;  // frequency
	private String s;  // coWord
	
	public ItemPair(double x, String s){
		this.x = x;
		this.s = s;
	}

	public double getFreq(){
		return x;
	}
		
	public String getWord(){
		return s;
	}
	public int compareTo(ItemPair other) {
		if(this.x > other.x) return 1;
		if(this.x == other.x) return 0;
		return -1;
	}
	
	public String toString(){
		return s + "\t" + x;
	}	
}