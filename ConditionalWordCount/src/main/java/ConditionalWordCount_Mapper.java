
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.yarn.*;


public class ConditionalWordCount_Mapper 
	extends Mapper<Object, Text, Text, IntWritable>{
	
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
    	
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        String str = word.toString();   // convert text to string
        str = str.toLowerCase();
        word.set(str);      // reset word to lower case
        
        word.set(ConditionalWordCount_Mapper.screenPunctuation(word)); //check punctuation
        context.write(word, one);
      }
    }
    
    
    public static Text screenPunctuation(Text word){
    	
    	String str = word.toString();
    	int end= str.length()-1 ;
    	char c1 = str.charAt(0);
    	char c2 = str.charAt(end);
    	
    	if(!Character.isAlphabetic(c1) && !Character.isAlphabetic(c2))
    		str = str.substring(1,end);
    	
    	else if(!Character.isAlphabetic(c1))
    		str = str.substring(1);
    	
    	else if(!Character.isAlphabetic(c2))
    		str = str.substring(0,end); 
    	
    	word.set(str);
    	
    	return word;
    }
    
    
    
    public static Text screenStem(Text word){
    	
    	String str = word.toString();
    	int end = str.length()-1;
    	System.out.println(str.substring(end-1));
    	
    	if(str.substring(end-2).equals("ing")){
    		str = str.substring(0,end-2);
    		word.set(str);
    	}
    	else if(str.substring(end-1).equals("ed")){
    		str = str.substring(0,end-1);
    		word.set(str);
    	}
    	else if(str.substring(end-1).equals("ly")){
    		str = str.substring(0,end-1);
    		word.set(str);
    	}
    	
    	return word;
    }
    
    
    
    

	
	
}
