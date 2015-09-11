import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ConditionalWordCount_Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	
	Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "conditional word count");
    job.setJarByClass(ConditionalWordCount_Driver.class);
    
    job.setMapperClass(ConditionalWordCount_Mapper.class);
    job.setCombinerClass(ConditionalWordCount_Reducer.class);
    job.setReducerClass(ConditionalWordCount_Reducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	
}
