import static org.junit.Assert.*;

import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import utilities.MyMapWritable;
import utilities.MyMapWritable2;

public class MyMapWritable_Test {
	
	// Fields
	// ------
	MyMapWritable2 m;

	
		@Before
		public void setUp(){
			m = new MyMapWritable2();
			m.put(new Text("world"), new IntWritable(3));
			
			m.put(new Text("hadoop"), new IntWritable(10));
			
			
			Text t = new Text("hi");
			IntWritable x = new IntWritable(5);
			m.put(t,x);
		}
	// TESTS
	// -----	
		
		@Test
		public void testSize(){
		
			Assert.assertFalse("isEmpty(): ", m.isEmpty());
			assertEquals("Size: ", 3, m.size());
			System.out.println(m.size());
			
		}
		
		
		@Test
		public void testToString(){
			String s = "world\t3\nhadoop\t10\nhi\t5\n";
			System.out.println(m.toString());
			assertEquals("toString: ", s, m.toString());
		}
		
		@Test 
		public void testIteration(){
			System.out.println("Test Iteration");
			System.out.println("--------------");
			String s1; 
			String s2;
			
			Set<Writable> keys = m.keySet();
			for(Writable w : keys){
				s1 = w.toString();  // key
				s2 = m.get(w).toString(); // value
				System.out.println(s1 + "\t" + s2);
				assertTrue(m.containsKey(w));
				assertTrue(m.containsValue(m.get(w)));
				
			}
		}
		
	

}
