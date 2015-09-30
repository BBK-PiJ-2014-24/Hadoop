package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;

public class IntWritableArray2  extends ArrayPrimitiveWritable {

	private int[] arr;
	
	public IntWritableArray2(int size){
		this.arr = new int[size];
	}
	
	public IntWritableArray2(int[] arr){
		this.arr = arr;
	}
	
	public int[] get(){
		return this.arr;
	}
	
	public void set(int[] arr){
		this.arr = arr;
	}
	
	public int length(){
		return arr.length;
	}
	
	
	public void write(DataOutput out) throws IOException{
		for(Integer i : arr){
			out.writeInt(i);
		}
	}
	
	
	public void readin(DataInput in) throws IOException{
		this.arr = new int[100];
		int k=0;
		for(Integer i : arr){
			arr[k] = i;
		}
	}
	
	
	@Override
	public String toString(){
		
		String s = "";
		int size = arr.length;
		int k=0;
		for(Integer i : arr){
			if (k == size -1)
				s += Integer.toString(i);
			else
				s+= Integer.toString(i) + ", ";

			k++;
		}
		return s;		
	}


	
	
	
	
}
