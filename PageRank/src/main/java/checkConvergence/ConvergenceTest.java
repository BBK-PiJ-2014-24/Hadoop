package checkConvergence;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class ConvergenceTest {
	
	public static boolean checkConvergence(File oldFile, File newFile){
			
		
		BufferedReader oldBr = null;
		BufferedReader newBr = null;
		float epsilon = 0.000001f;
		
		
		try{
			FileReader oldFr = new FileReader(oldFile);
			FileReader newFr = new FileReader(newFile);
			
			oldBr = new BufferedReader(oldFr);
			newBr = new BufferedReader(newFr);
		
			String lineOld = null;
			String lineNew = null;
		
			while( (lineOld=oldBr.readLine())!=null || (lineNew=newBr.readLine())!=null ){
				String[] oldElements = lineOld.split("\t");
				String[] newElements = lineNew.split("\t");
				System.out.println(oldElements[2]+"\t"+newElements[2]);
				
				float oldPr = Float.parseFloat(oldElements[2]);
				float newPr = Float.parseFloat(newElements[2]);
				
				float delta = Math.abs(newPr - oldPr);
				if(delta>epsilon)
					return false; // Convergence Test Failed
			}
		}
		catch(Exception ex1){System.out.println("File Error");}
		finally{
			try{
				oldBr.close();
				newBr.close();
			}
			catch(Exception ex2){
				System.out.println("BufferedReader Error");
			}
		}
		
		return true;  // Convergence Test Passed
	}
	

}
