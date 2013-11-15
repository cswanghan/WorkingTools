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
public class MD5CoderClick {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		MD5CoderClick m = new MD5CoderClick();
		m.FileProcessor(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
		//m.FileProcessor("externalsrc/click2", 6, 15);
	}

	public void FileProcessor(String input_file, int mac_position, int length) throws Exception
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
			StringBuilder output_str = new StringBuilder();
			for(int i = 0; i < length; i++)
			{
				if(details.length < 14)
					continue;
				else
				{
					if(i != 0)
						output_str.append(",");
					if(i == 6)	
						output_str.append(udid_md5);
					else if(i == length -1 )
					{
						if(details[i - 1] != null)
							output_str.append(MD5Converter(details[i - 1]));
						else 
							output_str.append("null");
					}
					else if(i != length -1)
						output_str.append(details[i]);
				}
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
