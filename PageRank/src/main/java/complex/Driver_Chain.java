package chain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.RunJar;




public class Driver_Chain extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		//Driver_Basic_Chain2.job1();
		//Driver_Basic_Chain2.job2();
	
		ToolRunner.run(new Configuration(), new Driver_Chain(), args);
	}
	
	
	
	/**
	 * Job for READ DATA
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	


	public int run(String[] args) throws Exception {
		
		// READER
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "The Reader Driver");
		job1.setJarByClass(Driver_Chain.class);

		job1.setMapperClass(Map_Reader3.class);
		//job.setCombinerClass(Pair_Reducer.class);
		job1.setReducerClass(Reduce_Reader.class);

		job1.setMapOutputKeyClass(IntWritable.class);  // Reader Map key Output
		job1.setMapOutputValueClass(IntWritable.class); // Reader Map Value Output
		job1.setOutputKeyClass(IntWritable.class);  // Reader Reducer Key Output
		job1.setOutputValueClass(Node.class); // Reader Reducer Value Output		
		//soc-Epinions1.txt.gz
		FileInputFormat.addInputPath(job1, new Path("./src/main/resources/Sample2.txt"));
		FileOutputFormat.setOutputPath(job1, new Path("./target/job1_Output/"));

		job1.waitForCompletion(true);
		
		
		// BASIC
		//---------
		
		Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "PageRank_Basic");
	    job2.setJarByClass(Driver_Chain.class);
	    
	    job2.setMapperClass(Mapper_Basic3.class);
	    //job.setCombinerClass(Pair_Reducer.class);
	    job2.setReducerClass(Reduce_Basic.class);
	   
	    
	  //  job2.setMapOutputKeyClass(IntWritable.class);  // Basic Mapper Output key
	//	job2.setMapOutputValueClass(Text.class);       // Basic Mapper Output Value
	    job2.setOutputKeyClass(IntWritable.class);     // Basic Reducer Output key
	    job2.setOutputValueClass(Node.class);		   // Basic Reducer Output Value	
	    
	    //job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    
	    
	    FileInputFormat.addInputPath(job2, new Path("./target/job1_Output/"));
	    FileOutputFormat.setOutputPath(job2, new Path("./target/job2_Output/"));
	    
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
		return 0;
	}

}
