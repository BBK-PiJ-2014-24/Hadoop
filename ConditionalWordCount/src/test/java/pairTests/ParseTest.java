package pairTests;

import utilities.ParseUtilities;

public class ParseTest {

	public static void main(String[] args) {
		
		String coWord = "Spain\"";
		
		coWord = ParseUtilities.screenPunctuation(coWord);
		System.out.println(coWord);

	}

}
