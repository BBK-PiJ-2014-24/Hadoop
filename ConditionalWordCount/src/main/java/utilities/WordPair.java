package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable{
	
	// Fields
	// ------
		
	private Text left;
	private Text right;
		
	// Constructor
	// -----------
		
	public WordPair(){
		this.left = new Text();
		this.right = new Text();
	}
	
	// Getter/Setters
	// --------------
	
	public void setLeft(String l){
		this.left.set(l);
	}
	
	public void setRight(String r){
		this.right.set(r);
	}
	
	public void set(String l, String r){
		this.left.set(l);
		this.right.set(r);
	}
	
	
	public Text getLeft(){
		return this.left;
	}
	
	public Text getRight(){
		return this.right;
	}
	


	// Serialization
	// -------------

	public void write(DataOutput out) throws IOException {
		this.left.write(out);
		this.right.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		this.left.readFields(in);
		this.right.readFields(in);
		
	}

	// Compare
	// -------
	
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

}
