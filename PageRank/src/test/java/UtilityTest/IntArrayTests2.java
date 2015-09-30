package UtilityTest;


import static org.junit.Assert.assertTrue;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert.*;
import org.junit.Assert;
// import org.junit.Assert.assertTrue;


import utilities.IntWritableArray2;

public class IntArrayTests2  {

	
	private int[] intArray;
	private IntWritableArray2 WritArr;
	
	
	@Before
	public void setUp(){
		intArray = new int[3];
		intArray[0] = 10;
		intArray[1] = 11;
		intArray[2] = 12;
		WritArr = new IntWritableArray2(intArray);
		
		
		
	}
	
	
	@Test
	public void test1(){
		System.out.println(WritArr.toString());

		Assert.assertEquals(WritArr.toString(),"10, 11, 12");
	}
	
	@Test
	public void test2(){
		Assert.assertEquals(intArray, WritArr.get());
	}
	
	
	
	
	
	
}
