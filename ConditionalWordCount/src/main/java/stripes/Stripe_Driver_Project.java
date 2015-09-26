/**
 * Driver for Project
 */
		


package stripes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utilities.MyMapWritable2;

public class Stripe_Driver_Project {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "stripe word count");
	    job.setJarByClass(Stripe_Driver.class);
	    
	    job.setMapperClass(Stripe_Mapper.class);
	    //job.setCombinerClass(Stripe_Reducer.class);
	    job.setReducerClass(Stripe_Reducer3.class);
	    
	 //   job.setMapOutputKeyClass(Text.class);
	 //   job.setMapOutputValueClass(MyMapWritable2.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(MyMapWritable2.class);
	    
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    Path inputFolder = new Path("./src/main/resources/JaneAusten/");
	    Path outputFolder = new Path("./target/JaneAusten/");
	    
	    FileInputFormat.addInputPath(job, inputFolder);
	    FileOutputFormat.setOutputPath(job, outputFolder);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		}
}
