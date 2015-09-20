package chainMaps;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
//import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.mapreduce.lib.chain.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import aggregate.ConditionalWordCount_Reducer;




public class Chain_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	
		Configuration conf = new Configuration();
		//JobConf job = new JobConf(conf);
		Job job = Job.getInstance(conf, "conditional word count");
		job.setJarByClass(Chain_Driver.class);
		
	//	job.setJobName("ChainJob");
	//	job.setInputFormat(TextInputFormat.class);
	//	job.setOutputFormat(TextOutputFormat.class);
		
	 //  FileInputFormat.setInputPaths(job, new Path(args[0]));
	 //  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	
	    
	    Configuration map1Conf = new Configuration(false);
	  //  JobConf map1Conf = new JobConf(false); // false turns off default configs.
	    ChainMapper.addMapper(job, Map1.class, 
	    					  IntWritable.class, Text.class, 
	    					  Text.class, Text.class, 
	    					  map1Conf);
	    
	    Configuration map2Conf = new Configuration(false);
	    //JobConf map2Conf = new JobConf(false);
	    ChainMapper.addMapper(job, Map2.class, 
	    					  Text.class, Text.class, 
	    					  Text.class, Text.class, 
	    					  map2Conf);
	    
	    Configuration map3Conf = new Configuration(false);
	    //JobConf map3Conf = new JobConf(false);
	    ChainMapper.addMapper(job, Map3.class, 
	    					  Text.class, Text.class, 
	    					  Text.class, Text.class, 
	    					  map3Conf);
	    
	    Configuration map4Conf = new Configuration(false);
	    //JobConf map4Conf = new JobConf(false);
	    ChainMapper.addMapper(job, Map4.class, 
	    					  Text.class, Text.class, 
	    					  Text.class, IntWritable.class, 
	    					  map4Conf);
	    
	    Configuration reduce2Conf = new Configuration(false);
	    //JobConf reduce1Conf = new JobConf(false);
	    ChainReducer.setReducer(job, Reduce2.class,
	    						Text.class, IntWritable.class,
	    						Text.class, IntWritable.class,
	    						reduce2Conf);
	    
	    
	 //   job.waitForCompletion(true);
	  /*  JobClient.runJob(map1Conf);
	    JobClient.runJob(map2Conf);
	    JobClient.runJob(map3Conf);
	    JobClient.runJob(map4Conf);
	    JobClient.runJob(reduce1Conf);
	    */
	    job.submit();

	    
	    
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	    
	  
	    
	}
	
	
}
