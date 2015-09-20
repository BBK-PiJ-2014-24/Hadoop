/**
 * With in-Mapping there is no need for combiner as already combining combiner functionality within
 * the mapper as all opportunities for local aggregattion exhausted.
 */


package inMap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class inMapDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "in-Map word count");
	    job.setJarByClass(inMapDriver.class);
	    
	    job.setMapperClass(inMapMapper2.class);
	    //job.setCombinerClass(.class);
	    job.setReducerClass(inMapReduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		}
	
}
