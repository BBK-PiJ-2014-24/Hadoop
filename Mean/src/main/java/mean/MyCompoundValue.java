/**
 * A Writable is used for making your own Compund Value Objects. Just
 * a container for transporter for carrying multiple variables in the
 * MR. MyCompoundKey as it will serve as a compound value. As such, it 
 * implements Writable. Nb. A CompoundKey would need to implement 
 * WritableComparable (as the keys will need to be compared.)
 */

package mean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MyCompoundValue implements Writable{

	// Fields
	// ------
	
	private int total;
	private int count;
	
	// Constructor
	// -----------
	
	private MyCompoundValue(){
		total = 0;
		count = 0;
	}
	
	
	public MyCompoundValue(int total, int count){
		this.total = total;
		this.count = count;
	}
	

	// Serializes the Data
	// -------------------
	public void write(DataOutput out) throws IOException {
		out.writeInt(total);
		out.writeInt(count);
	}
	
	
	// Deserializes the Data
	// ---------------------
	public void readFields(DataInput in) throws IOException {
		total = in.readInt();
		count = in.readInt();
	}

	
	// Static Way to Call a Constructor
	// --------------------------------
	// readFields(in) Will set the Fields
	public static MyCompoundValue read(DataInput in) throws IOException{
		MyCompoundValue cv = new MyCompoundValue();
		cv.readFields(in);
		return cv;
	}
	
	
	
	
	

}
