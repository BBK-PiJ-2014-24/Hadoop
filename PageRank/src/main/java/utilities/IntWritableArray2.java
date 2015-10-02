package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;

public class IntWritableArray2  extends ArrayPrimitiveWritable {

	private int[] arr;
	private int size;
	
	public IntWritableArray2(){
		super();
	}
	
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
		
		
	this.write(out);	
	//	out.writeInt(this.size);
	//	for(int i=0;i<size;i++){
	//		out.writeInt(this.arr[i]);
	//	}
	//	this.write(out);
	}
	
	
	public void readfields(DataInput in) throws IOException{
		
	this.readfields(in);	
		//adjListArray.write
		
	//	size = in.readInt();
	//	this.arr = new int[size];
	//	for(int i=0; i<size; i++){
	//		this.arr[i] = in.readInt();
	//	} 
	}
	
	
	public int compareTo(Object o){
		
		IntWritableArray2 other = (IntWritableArray2) o;
		//return this.size > other.length() ? 1 : 0;
		//this.size > other.length();
			return this.size - other.length();
	
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
