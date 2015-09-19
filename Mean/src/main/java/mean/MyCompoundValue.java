/**
 * A Writable is used for making your own Compund Value Objects. Just
 * a container for transporter for carrying multiple variables in the
 * MR.
 */

package mean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MyCompoundValue implements Writable{

	private int total;
	private int count;
	
	public MyCompoundValue(int total, int count){
		this.total = total;
		this.count = count;
	}
	

	public void write(DataOutput out) throws IOException {
		out.writeInt(total);
		out.writeInt(count);
	}

	public void readFields(DataInput in) throws IOException {
		total = in.readInt();
		count = in.readInt();
	}

	
	
	
	
	
	
	

}
