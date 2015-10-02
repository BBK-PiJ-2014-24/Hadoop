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
	private int[] otherArray;
	private IntWritableArray2 writArr;
	private IntWritableArray2 otherWritArr;
	
	
	@Before
	public void setUp(){
		intArray = new int[3];
		intArray[0] = 10;
		intArray[1] = 11;
		intArray[2] = 12;
		writArr = new IntWritableArray2(intArray);	
		
		otherArray = new int[]{50};
		otherWritArr = new IntWritableArray2(otherArray);	
		
	}
	
	/**
	 * Test of toString() of elements in array
	 */
	@Test
	public void test1(){
		System.out.println(writArr.toString());

		Assert.assertEquals(writArr.toString(),"10, 11, 12");
	}
	
	/**
	 * Test Correct Elements!
	 */
	
	@Test
	public void test2(){
		Assert.assertEquals(intArray, writArr.get());
	}
	
	
	/*
	 * Test compareTo()
	 */
	@Test
	public void test3(){
		
		System.out.println("compareTo");
		System.out.println(writArr.compareTo(otherWritArr));
		
	}
	
	
	
	
}
