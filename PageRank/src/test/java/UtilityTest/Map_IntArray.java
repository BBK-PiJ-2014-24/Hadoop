/**
 * Simple Map to test the IntWritableArray2. Map Just Returns itself.
 */


package UtilityTest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import utilities.IntWritableArray2;

public class Map_IntArray extends Mapper<IntWritable, IntWritableArray2, IntWritable, IntWritableArray2> {

	public void map(IntWritable key, IntWritableArray2 value, Context context) throws IOException, InterruptedException{
		
		
		
		context.write(key, value);
		
	}
	
	
}
