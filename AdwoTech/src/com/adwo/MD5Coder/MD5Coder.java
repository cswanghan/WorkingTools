/**
 * 
 */
package com.adwo.MD5Coder;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.security.MessageDigest;

/**
 * @author dev
 *
 */
public class MD5Coder {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		MD5Coder m = new MD5Coder();
		m.FileProcessor(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
	}

	public void FileProcessor(String input_file, int mac_position, int idfa_position) throws Exception
	{
		InputStreamReader InputFile = new InputStreamReader(
				new FileInputStream(input_file), "UTF-8");
		BufferedReader FileReader = new BufferedReader(InputFile);
		String temp = null;
		while((temp = FileReader.readLine()) != null)
		{
			String[] details = temp.split(",");
			String udid = details[mac_position];
			String udid_md5 = MD5Converter(udid);
			String idfa = details[idfa_position];
			String idfa_md5 = MD5Converter(idfa);
			StringBuilder output_str = new StringBuilder();
			for(int i = 0; i < 13; i++)
			{
				if(i != 0)
					output_str.append(",");
				if(i == 6)	
					output_str.append(udid_md5);
				else if(i == 8)
					output_str.append(idfa_md5);
				else 		
					output_str.append(details[i]);
			}
			System.out.println(output_str);
		}
	}
	
	public String MD5Converter(String input_str)
	{
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9','a', 'b', 'c', 'd', 'e', 'f' }; 
		try {
			   byte[] strTemp = input_str.getBytes();
			   //使用MD5创建MessageDigest对象
			   MessageDigest mdTemp = MessageDigest.getInstance("MD5");
			   mdTemp.update(strTemp);
			   byte[] md = mdTemp.digest();
			   int j = md.length;
			   char str[] = new char[j * 2];
			   int k = 0;
			   for (int i = 0; i < j; i++) {
				    byte b = md[i];
				    str[k++] = hexDigits[b >> 4 & 0xf];
				    str[k++] = hexDigits[b & 0xf];
			   }
			   return new String(str); 
		}catch(Exception e)
		{
			return null;
		}
	}
}
