import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TreeTest {

	
	
	public static void main(String[] args) {
		
		TreeMap<IntWritable, Text> rankTree = new TreeMap<IntWritable, Text>();
		
		rankTree.put(new IntWritable(1), new Text("one"));
		rankTree.put(new IntWritable(2), new Text("two"));
		rankTree.put(new IntWritable(3), new Text("three"));
		rankTree.put(new IntWritable(4), new Text("four"));
		
		for(Map.Entry<IntWritable, Text> entry : rankTree.entrySet()){
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}

	}

}
