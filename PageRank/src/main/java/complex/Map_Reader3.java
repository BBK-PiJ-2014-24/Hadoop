package chain;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map_Reader3 extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	
	
	
	@Override
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
		
		
		
		StringTokenizer st = new StringTokenizer(values.toString(), "\n");
		int countTok = 0;		
			while (st.hasMoreTokens()){
				String line = st.nextToken();
				countTok++;
				System.out.println("countTok " + countTok);
				System.out.println("LINE " + line);
				
				char c = line.charAt(0);
				System.out.println("char = " +  Character.isDigit(c)); 
				if(Character.isDigit(c)){  // check this is a line of data
					String[] parts = line.split("\\s+");
					System.out.println(parts[0] +"\t\t\t"+ parts[1]);
					int id = Integer.parseInt(parts[0]);
					int edge = Integer.parseInt(parts[1]);
					context.write(new IntWritable(id), new IntWritable(edge));
				}	
			}
	}	

}
