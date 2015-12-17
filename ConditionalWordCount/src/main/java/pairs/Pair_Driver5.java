package pairs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utilities.WordPair;

public class Pair_Driver5{
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "pair for Cond Prob + TopN");
	    job.setJarByClass(Pair_Driver5.class);
	    
	    job.setMapperClass(Pair_Mapper3.class);
	    //job.setCombinerClass(Pair_Reducer.class);
	    job.setReducerClass(Pair_Reducer4.class);
	    
	    job.setOutputKeyClass(WordPair.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		}
}
