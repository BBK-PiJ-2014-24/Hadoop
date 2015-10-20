/**
 * Check for Convergence of PageRank by comparing current and previous Iteration. 
 */

package checkConvergence;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class Convergence {
	
	public static boolean check(File oldFile, File newFile){
			
		
		BufferedReader oldBr = null;
		BufferedReader newBr = null;
		FileReader oldFr = null;
		FileReader newFr = null;
		float epsilon = 0.001f;
		
		System.out.println("+++++CHECK CONVERGENCE ++++++++++");
		
		try{
			oldFr = new FileReader(oldFile);
			newFr = new FileReader(newFile);
			
			
			oldBr = new BufferedReader(oldFr);
			newBr = new BufferedReader(newFr);
		
			String lineOld = "";
			String lineNew = "";
			 //  
			while( (lineOld=oldBr.readLine())!=null ){
				lineNew = newBr.readLine();
				String[] oldElements = lineOld.split("\t");
				String[] newElements = lineNew.split("\t");
				
				//System.out.println(oldElements[3]+"\t"+newElements[3]);
				
				float oldPr = Float.parseFloat(oldElements[3]);
				float newPr = Float.parseFloat(newElements[3]);
				System.out.println(oldPr + "\t" + newPr);
				
				float delta = Math.abs(newPr - oldPr);
				System.out.println("delta = " + delta);
				if(delta > epsilon){
					System.out.println("false");
					return false; // Convergence Test Failed
				}
			}
		}
		catch(Exception ex1){System.out.println("File Not Found Exception Error");}
		finally{
			try{
				oldBr.close();
				newBr.close();
			}
			catch(Exception ex2){
				System.out.println("BufferedReader Error");
			}
		}
		
		System.out.println("++++++ CONVERGED !!!!");
		return true;  // Convergence Test Passed
	}
	

}
