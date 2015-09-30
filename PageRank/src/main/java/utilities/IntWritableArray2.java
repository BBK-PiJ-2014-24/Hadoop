package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;

public class IntWritableArray2  extends ArrayPrimitiveWritable {

	private int[] arr;
	private int size;
	
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
		
		this.size = arr.length;
		return this.size;
		
	}
	
	
	public void write(DataOutput out) throws IOException{
		
		out.writeInt(this.size);
		for(int i=0;i<size;i++){
			out.writeInt(this.arr[i]);
		}
	//	this.write(out);
	}
	
	
	public void readin(DataInput in) throws IOException{
		
		size = in.readInt();
		this.arr = new int[size];
		for(int i=0; i<size; i++){
			this.arr[i] = in.readInt();
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
