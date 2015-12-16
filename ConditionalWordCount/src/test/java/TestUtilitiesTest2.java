
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
		
		System.out.println( ParseUtilities.screenPunctuation(new Text("hello")));
		assertEquals(ParseUtilities.screenPunctuation(new Text("hello")), new Text("hello"));
		assertEquals(ParseUtilities.screenPunctuation(new Text("hello!")), new Text("hello"));
		assertEquals(ParseUtilities.screenPunctuation(new Text("hello.")), new Text("hello"));
		assertEquals(ParseUtilities.screenPunctuation(new Text("'hello")), new Text("hello"));
		assertEquals(ParseUtilities.screenPunctuation(new Text("\"hello\"")), new Text("hello"));
		System.out.println(ParseUtilities.screenPunctuation(new Text("hello--a")));
		assertEquals(ParseUtilities.screenPunctuation(new Text("hello--a")), new Text("hello"));
		
		
	}
	
	
	
	
}
