package reader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utilities.Node;



public class Driver_Reader {

public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "The Reader Driver");
	    job.setJarByClass(Driver_Reader.class);
	    
	    job.setMapperClass(Map_Reader.class);
	    //job.setCombinerClass(Pair_Reducer.class);
	    job.setReducerClass(Reduce_Reader.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Node.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		}
}
