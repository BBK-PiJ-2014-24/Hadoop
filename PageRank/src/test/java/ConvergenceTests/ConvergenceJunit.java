package ConvergenceTests;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import checkConvergence.Convergence;
import static org.junit.Assert.*;

public class ConvergenceJunit {
	
	File oldFile;
	File newFile;
	
	@Before
	public void setUp(){
		oldFile = new File("/home/hduser/git/Hadoop/PageRank/target/job3_Output/part-r-00000");
		newFile = new File("/home/hduser/git/Hadoop/PageRank/target/job4_Output/part-r-00000");
		
	}
	
	@Test
	public void test1(){
		boolean isConverge =  Convergence.check(oldFile, newFile);
		assertFalse(isConverge);
	}
	

}
