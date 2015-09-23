package stripes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import utilities.MyMapWritable;
import utilities.MyMapWritable2;



public class Stripe_Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "stripe word count");
	    job.setJarByClass(Stripe_Driver.class);
	    
	    job.setMapperClass(Stripe_Mapper.class);
	    job.setCombinerClass(Stripe_Reducer.class);
	    job.setReducerClass(Stripe_Reducer.class);
	    
	 //   job.setMapOutputKeyClass(Text.class);
	 //   job.setMapOutputValueClass(MyMapWritable2.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(MyMapWritable2.class);
	    
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		}
}
