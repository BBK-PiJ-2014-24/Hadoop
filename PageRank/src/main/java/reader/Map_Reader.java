package reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import utilities.Node;
import utilities.NodeType;

public class Map_Reader extends Mapper<IntWritable, IntWritable, IntWritable, Node>{

	// Fields
	// ------
	BufferedReader br;
	
	@Override
	public void setup(Context context) throws IOException{
        Path pt=new Path("./src/main/resources/Sample.txt");//Location of file in HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        
        
    }  // soc-Epinions1.txt.gz
	
	@Override
	public void map(IntWritable key, IntWritable values, Context context) throws IOException, InterruptedException{
		
		Node n = new Node();
		n.setNodeType(NodeType.ProbMass);
		n.setPageRank(0.33f);
		
		String line;
		line=br.readLine();
        while (line != null){
            System.out.println(line);
            line=br.readLine();
        }
        
        
		
		
		context.write(key, n);
	}
	
}
