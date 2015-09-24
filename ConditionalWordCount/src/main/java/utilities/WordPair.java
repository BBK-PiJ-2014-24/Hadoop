package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordPair implements WritableComparable<WordPair>{
	
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
	
	public int compareTo(WordPair other) {
		
		// check same left word
		int sameFirst = this.left.toString().compareTo(other.left.toString());
		if(sameFirst !=0)
			return sameFirst;
		
	
				
		if(this.right.toString().equals("*"))		
			return -1;
		if(other.right.toString().equals("*"))
			return 1;
		return 0;
	}
	
	
	public String toString(){
		return left.toString() + ", " + right.toString();
	}

}
