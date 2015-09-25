package pairs;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import utilities.WordPair;

public class Pair_Reducer3 extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {

	private Text star = new Text("*");
	private IntWritable result = new IntWritable();
	private DoubleWritable marginalCount = new DoubleWritable();
	
	
	
	// key is ordered with (for,*) on top, because of the comparable
	public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
	
		
		// find the (for,*) first to calc marginalCount FIRST 
		if(key.getRight().equals(star)){
			marginalCount.set(marginalCount.get() + calcSum(values));
			System.out.println("**** " + marginalCount.get());
		}
		else{
			int count = calcSum(values);
			double ans = (count*1.0)/marginalCount.get();
			ans = round(ans,4);
			DoubleWritable freq = new DoubleWritable();
			freq.set(ans);
			System.out.println("++++++ " + marginalCount.get());
			context.write(key, freq);
		}
	
	}
	
	
	// Method to calc any key's Partial sums by iterating its values
	public int calcSum(Iterable<IntWritable> values){
		int marg = 0;
		for(IntWritable i : values){
			marg += i.get();
		}
		return marg;
	}
	
private double round(double d, int dp){
		
		BigDecimal bd = new BigDecimal(d);
		bd = bd.setScale(dp, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}
		
}	