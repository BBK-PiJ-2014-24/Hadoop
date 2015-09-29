/**
 * Testing working order of IntWritableArray
 */

package UtilityTest;



import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert.*;

import static org.junit.Assert.*;

import java.util.Arrays;

import utilities.IntWritableArray;

public class IntArrayTests {

	
	private IntWritableArray intArr;
	private IntWritable[] arr;
	private ArrayWritable wa;
	
	@Before
	public void setUp(){
		intArr = new IntWritableArray();
		
		arr = new IntWritable[3];
		arr[0] = new IntWritable(10);
		arr[1] = new IntWritable(11);
		arr[2] = new IntWritable(12);
		
		
		intArr.set(arr);
		
	}
	
	/**
	 * Test length of Array
	 */
	@Test
	public void test1(){
		System.out.println("size: " + intArr.length());

		assertEquals("Check Array length: ", intArr.length(), 3);
		
	}
	
	/**
	 * Test Elements 
	 */
	@Test
	public void test2(){
		IntWritable[] innerArray = intArr.get();
	
		int k=0;
		for(IntWritable i : innerArray){
			assertTrue(Arrays.asList(innerArray).contains(arr[k]));
			System.out.println(arr[k]);
			k++;
		}
	}
	
	
}
