
import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import utilities.ParseUtilities;

import junit.framework.Assert;

public class TestUtilitiesTest2 extends TestUtilities {

	
	@Before
	public void setUp(){
		
	}
	
	
	@Test
	public void test1() {
		
		String s = "hello--a";
		for(int i=0; i< s.length() - 1; i++){
			System.out.println(s.charAt(i));
		}
		
		
		assertEquals(ParseUtilities.screenPunctuation("hello"), "hello");
		assertEquals(ParseUtilities.screenPunctuation("hello!"), "hello");
		assertEquals(ParseUtilities.screenPunctuation("hello."), "hello");
		assertEquals(ParseUtilities.screenPunctuation("'hello"), "hello");
		assertEquals(ParseUtilities.screenPunctuation("\"hello\""), "hello");


		ParseUtilities.screenPunctuation("hello--");
		System.out.println(ParseUtilities.screenPunctuation("hello--a"));
		assertEquals(ParseUtilities.screenPunctuation("hello--a"), "hello");
		System.out.println(ParseUtilities.screenPunctuation("hello--"));
		assertEquals(ParseUtilities.screenPunctuation("hello--"), "hello");
		
		
	}

	
	
	@Test
	public void test2() {
		
		
		
		assertEquals(ParseUtilities.screenPunctuation("us"), "us");
		assertEquals(ParseUtilities.screenPunctuation("us!"), "us");
		assertEquals(ParseUtilities.screenPunctuation("us."), "us");
		assertEquals(ParseUtilities.screenPunctuation("us_"), "us");
		assertEquals(ParseUtilities.screenPunctuation("'us"), "us");
		assertEquals(ParseUtilities.screenPunctuation("\"us\""), "us");


		ParseUtilities.screenPunctuation("us--");
		System.out.println(ParseUtilities.screenPunctuation("us--a"));
		assertEquals(ParseUtilities.screenPunctuation("us--a"), "us");
		System.out.println(ParseUtilities.screenPunctuation("us--"));
		assertEquals(ParseUtilities.screenPunctuation("us--"), "us");
		
		
	}
	
	
	@Test
	public void test3() {
		
		System.out.println( ParseUtilities.screenPunctuation(new Text("hello")));
		assertEquals(ParseUtilities.screenPunctuation(new Text("hello")), new Text("hello"));
		assertEquals(ParseUtilities.screenPunctuation(new Text("hello!")), new Text("hello"));
		assertEquals(ParseUtilities.screenPunctuation(new Text("hello.")), new Text("hello"));
		assertEquals(ParseUtilities.screenPunctuation(new Text("'hello")), new Text("hello"));
		assertEquals(ParseUtilities.screenPunctuation(new Text("\"hello\"")), new Text("hello"));
		System.out.println(ParseUtilities.screenPunctuation(new Text("hello--a")));
		assertEquals(ParseUtilities.screenPunctuation(new Text("hello--a")), new Text("hello"));
		
		
	}
	
	
	@Test 
	public void test4(){
		assertEquals(ParseUtilities.screenStem("it's"),"it");
		assertEquals(ParseUtilities.screenStem("women's"),"women");
		assertEquals(ParseUtilities.screenStem("orderly"),"order");
		assertEquals(ParseUtilities.screenStem("bucking"),"buck");
		assertEquals(ParseUtilities.screenStem("bucked"),"buck");
		
	}
	
}
