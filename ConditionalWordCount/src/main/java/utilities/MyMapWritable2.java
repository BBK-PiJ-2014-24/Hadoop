package utilities;

import java.util.Set;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MyMapWritable2 extends MapWritable {

	
	public MyMapWritable2(){
		super();
	}
	
	@Override
	public String toString(){
		
		String s = "";
		
		Set<Writable> keys = keySet();
		for(Writable key : keys){
			s+= "\n" + key.toString() + "\t" + get(key).toString();
		}
		return s;
	}
}
