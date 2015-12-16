
/**
 * Class that holds Static Methods for parsing
 * 1) screenPunctuation - screens punctuation
 * 2) screenStems -screens for stem words
 */


package utilities;

import org.apache.hadoop.io.Text;

public class ParseUtilities {

	  /**
     * Method to screen out punctuation
     * @param word
     * @return
     */
    
    public static Text screenPunctuation(Text word){
    	
    	String str = word.toString();
    	String cleanString = screenPunctuation(str);
    	Text cleanText = new Text(cleanString);	
    	return cleanText;
    }

    public static String screenPunctuation(String word){
    	
    	String str = word;
    	int end;
    	
    	
    	if(str.length() > 1){
    		end = str.length()-1 ;   // set end index
    	}
    	else 
    		return str;         	// single letter
    	
    	
       	for(int i=0;i<end;i++){				// Screen for  --
    		if(str.charAt(i) == '-' && str.charAt(i+1) == '-'){
    			str = str.substring(0,i);
    			end = str.length()-1 ;   // re-set end index
    			if(end > 0)
    				break;
    			else
    				return str;   // single letter
    		}
    	}	
    	
       	
       	
    	char c1 = str.charAt(0);
    	char c2 = str.charAt(end);
    	
    	if(!Character.isAlphabetic(c1) && !Character.isAlphabetic(c2) && str.length() > 2)  // Double apostrophe
    		str = str.substring(1,end);
    	
    	else if(!Character.isAlphabetic(c1))   // Single Quote
    		str = str.substring(1);
    	
    	else if(!Character.isAlphabetic(c2))   // Punctuation
    		str = str.substring(0,end); 
    	


    	return str;
    }    
    
    
    
    /**
     * Method to reduce words to stems. Screen works on -ed, -ly, -ing. 
     * Screen only works on words greater than three letters.
     * @param word
     * @return
     */
    public static String screenStem(String str){
    	
    	int end = str.length()-1;
    	
    	if(end+1 > 2  && str.charAt(end-1) == '\''){   // check for possessive noun
    		str = str.substring(0, end-1);
    		end = str.length()-1;
    	}
  
    	if(end+1 > 3){
	    	// -ing screen with min word length to avoid r-ing, 
	    	if(str.substring(end-2).equals("ing") && end+1 > 5){
	    		str = str.substring(0,end-2);
	    	}
	    	else if(str.substring(end-1).equals("ed")){
	    		str = str.substring(0,end-1);
	  	    	}
	    	// -ly screen with min word length to avoid f-ly
	    	else if(str.substring(end-1).equals("ly")){
	    		str = str.substring(0,end-1);
	    	}
    	}
    	
    	return str;
    }

	
	
	
	
}
