package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MyMapWritable extends MapWritable{
	
	// Fields
	// ------
	

	private MapWritable m;
	
	// Constructors
	// ------------
	public MyMapWritable(){
		m = new MapWritable();
		m.clear();
	}
	
	
	public void put(Text word, IntWritable count){
		m.put(word, count);
	}

	
	public int size(){
		return m.size();
	}
	
	public boolean isEmpty(){
		return m.isEmpty();
	}
	
	public boolean containsKey(Object key){
		return m.containsKey(key);
	}
	
	public boolean containsValue(Object value){
		return m.containsValue(value);
	}
	
	
	public Set<Writable> keySet(){
		return m.keySet();
	}
	
	
	public Writable get(Object key){
		return m.get(key);
	}
	
	public Collection<Writable> values(){
		return m.values();
	}
	
	
	public void readFields(DataInput in) throws IOException{
		m.readFields(in);
	}
	
	
	public void write(DataOutput out) throws IOException{
		m.write(out);
	}
	
	
	@Override
	public String toString(){
		
		String s = "";
		
		Set<Writable> keys = m.keySet();
		for(Writable key : keys){
			s+= key.toString() + "\t" + m.get(key).toString() + "\n";
		}
		return s;
	}

}
