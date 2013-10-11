/**
 * 
 */
package com.adwo.GetIDFA;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * @author dev
 *
 */
public class getIDFA {

	public void GetIDFA(String filename, int datatype) throws Exception
	{
		String input = filename;
		InputStreamReader InputFile = new InputStreamReader(new FileInputStream(input),"UTF-8");
		BufferedReader FileReader = new BufferedReader(InputFile);
		
		String temp = null;
		while((temp = FileReader.readLine()) != null)
		{
			String proposedIDFA = null;
			String mac = null;
			if(datatype == 1)
			{
				//click
				String[] details = temp.split(",");
				int leg = details.length;
				if(details[leg - 1].contains("-"))
					proposedIDFA = details[leg - 1];
				mac = details[5];
			}
			else if(datatype == 2)
			{
				//show
				String[] details = temp.split(",");
				int leg = details.length;
				if(details[leg - 4].contains("-"))
					proposedIDFA = details[leg - 4];
				mac = details[5];
			}
			System.out.println(mac + "::" + proposedIDFA);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		getIDFA gt = new getIDFA();
		gt.GetIDFA(args[0], Integer.valueOf(args[1]));
	}

}
