package utilities;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntWritableArray extends ArrayWritable{

	
	public IntWritableArray(){
		super(IntWritable.class); // Constructor is passed class of Writable
	}
	
	
}
