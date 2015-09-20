/**
 *  Class for static utility methods to help you test files in MRUnit
 */


import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import chainMaps.Map5;

public class TestUtilities {

	static List<Pair<LongWritable, Text>> getInput(){
	    Scanner scanner = new Scanner(Map5.class.getResourceAsStream("src/test/resources/input.txt"));
	    List<Pair<LongWritable, Text>> input = new ArrayList<Pair<LongWritable, Text>>();
	    while(scanner.hasNext()){
	        String line = scanner.nextLine();
	        input.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text(line)));
	    }
	    scanner.close();
	    return input;
	}
	 
	private static int getInputLines(){
	    Scanner scanner = new Scanner(Map5.class.getResourceAsStream("src/test/resources/input.txt"));
	    int line = 0;
	    while(scanner.hasNext()){
	        scanner.nextLine();
	        line++;
	    }
	    scanner.close();
	    return line;
	}
	 
	private static int getOutputRecords(){
	    Scanner scanner = new Scanner(Map5.class.getResourceAsStream("output/wordcount-output.txt"));
	    int record = 0;
	    while(scanner.hasNext()){
	        scanner.nextLine();
	        record++;
	    }
	    scanner.close();
	    return record;
	}
	 
	private static List<Pair<Text,IntWritable>> getOutput(String fileName){
	    Scanner scanner = new Scanner(Map5.class.getResourceAsStream(fileName));
	    List<Pair<Text,IntWritable>> output = new ArrayList<Pair<Text, IntWritable>>();
	    while(scanner.hasNext()){
	        String keyValue[] = scanner.nextLine().split(",");
	        String word = keyValue[0];
	        String count = keyValue[1];
	        output.add(new Pair<Text, IntWritable>(new Text(word), new IntWritable(Integer.parseInt(count))));
	    }
	    scanner.close();
	    return output;
	}
	
	
}
