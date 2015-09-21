import org.apache.hadoop.io.Text;

import utilities.MyMapWritable;

public class MyMapWritable_Test {
	
	public static void main(String[] args){
		
		Text t = new Text("hi");
	
		
		MyMapWritable m = new MyMapWritable("Java", 2);
		System.out.println(m.toString());
	}

}
