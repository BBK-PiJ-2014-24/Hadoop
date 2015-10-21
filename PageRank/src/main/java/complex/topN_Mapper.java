/*
 * Mapper for calc topN
 */

package chain;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class topN_Mapper extends Mapper<LongWritable, Text, IntWritable, Node>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		Node n = Node.create(value.toString());
		System.out.println("topN MAPPER: " + n.toString());
		context.write(new IntWritable(n.getNodeID()), n);
	}

}
