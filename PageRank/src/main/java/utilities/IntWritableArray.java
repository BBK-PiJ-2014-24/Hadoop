package utilities;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntWritableArray extends ArrayWritable{

	
	public IntWritableArray(){
		super(IntWritable.class); // Constructor is passed class of Writable
	}
	
	@Override
	public IntWritable[] get(){
		return (IntWritable[]) super.get();
	}
	
/*	
	@Override
	public void write(DataOutput out) throws IOException{
		for(IntWritable i : get()){
			i.write(out);
		}
	}
*/
	
	
	@Override
	public String toString(){
		
		String s = "";
		IntWritable[] values = get();
		for(IntWritable i : values){
			s = Integer.toString(i.get());
		}	
		return s;
	}
	
	
}
