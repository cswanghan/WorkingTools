/**
 * 
 */
package com.adwo.SDKconverter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * @author dev
 *
 */
public class SDKconverter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SDKconverter sc = new SDKconverter();
		if(args.length < 3 || args.length > 3)
			System.out.println("Wrong input paramerter, Correct Format: java -jar XXX.jar <input_file> <regex> <print_position>");
		else
			sc.Converter(args[0], args[1], args[2]);
	}
	
	public void Converter(String inputfile, String regex, String pos)
	{
		InputStreamReader InputFile;
		try {
			InputFile = new InputStreamReader(
					new FileInputStream(inputfile), "UTF-8");
			BufferedReader FileReader = new BufferedReader(InputFile);
			String temp = null;
			while((temp = FileReader.readLine()) != null)
			{
				String[] details = temp.split(regex);
				System.out.println(Integer.valueOf(details[Integer.valueOf(pos)])&0x00FF);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
