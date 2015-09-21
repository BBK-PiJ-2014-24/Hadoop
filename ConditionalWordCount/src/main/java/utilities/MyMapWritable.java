package utilities;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class MyMapWritable extends MapWritable{
	
	// Fields
	// ------
	
	private Text word;
	private IntWritable count;
	private MapWritable m;
	
	// Constructors
	// ------------
	public MyMapWritable(){
		super();
	}
	
	public MyMapWritable(String s, int x){
		this.word = new Text(s);
		this.count = new IntWritable(x);
		m = new MapWritable();
		m.put(word,count);
	}
	
	public MyMapWritable(Text w, IntWritable x){
		this.word = w;
		this.count = x;
		m = new MapWritable();
		m.put(word,count);
	}
	

	
	
	@Override
	public String toString(){
		return word.toString() + "\t" + count.toString();
	}

}
