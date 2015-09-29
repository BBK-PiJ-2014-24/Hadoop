/**
 * Testing working order of IntWritableArray
 */

package UtilityTest;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

import utilities.IntWritableArray;

public class IntArrayTests {

	
	private IntWritableArray intArr;
	private ArrayWritable wa;
	
	@Before
	public void setUp(){
		intArr = new IntWritableArray();
		
		IntWritable[] arr = new IntWritable[3];
		arr[0] = new IntWritable(10);
		arr[1] = new IntWritable(11);
		arr[2] = new IntWritable(12);
		
		
		intArr.put(new IntWritable(10));
		intArr.set(new IntWritable(11));
		intArr.set(new IntWritable(12));
	}
	
	@Test
	public void test1(){
		
		
	}
	
	
}
