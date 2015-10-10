package reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utilities.Node;
import utilities.NodeType;

public class Map_Reader2 extends Mapper<Object, IntWritable, IntWritable, IntWritable>{

	
	
	
	@Override
	public void map(Object key, IntWritable values, Context context) throws IOException, InterruptedException{
		
		StringTokenizer st = new StringTokenizer(values.toString(), "\t");
				
			while (st.hasMoreTokens()){
				String nodeid = st.nextToken();
				char c = nodeid.charAt(0);
				if(Character.isDigit(c) && st.hasMoreTokens()){  // check this is a line of data
					String nodeEdge = st.nextToken();
					System.out.println(nodeid +"\t\t\t"+ nodeEdge);
					int id = Integer.parseInt(nodeid);
					int edge = Integer.parseInt(nodeEdge);
					context.write(new IntWritable(id), new IntWritable(edge));
				}	
			}
	}	
}
