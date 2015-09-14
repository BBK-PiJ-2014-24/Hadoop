
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
        word.set(ConditionalWordCount_Mapper.screenStem(word)); // screen stems
        context.write(word, one);
      }
    }
    
    /**
     * Method to screen out punctuation
     * @param word
     * @return
     */
    
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
    
    
    /**
     * Method to reduce words to stems. Screen works on -ed, -ly, -ing. 
     * Screen only works on words greater than three letters.
     * @param word
     * @return
     */
    public static Text screenStem(Text word){
    	
    	String str = word.toString();
    	int end = str.length()-1;
  
    	if(end+1 > 3){
	    	// -ing screen with min word length to avoid r-ing, 
	    	if(str.substring(end-2).equals("ing") && end+1 > 5){
	    		str = str.substring(0,end-2);
	    		word.set(str);
	    	}
	    	else if(str.substring(end-1).equals("ed")){
	    		str = str.substring(0,end-1);
	    		word.set(str);
	    	}
	    	// -ly screen with min word length to avoid f-ly
	    	else if(str.substring(end-1).equals("ly")){
	    		str = str.substring(0,end-1);
	    		word.set(str);
	    	}
    	}
    	
    	return word;
    }
    
    
    
    

	
	
}
