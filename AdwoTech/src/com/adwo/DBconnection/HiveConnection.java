package com.adwo.DBconnection;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
public class HiveConnection {

	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	private static String db_driver = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	public static Connection getConnection() throws Exception
	{
			    
	    	/*Properties prop = new Properties();
	    	FileInputStream fis = new FileInputStream("externalsrc/db.properties");  
		prop.load(fis);
		String url = prop.getProperty("hive_url");
	    	prop.put("user", prop.getProperty("hive_username"));
	    	prop.put("password",prop.getProperty("hive_password"));
	    	prop.put("charSet","UTF-8");

	    	Class.forName(db_driver);	    

	    	Connection conn = DriverManager.getConnection(url, prop);
	    	
	    	if(!conn.isClosed()) 
	    		System.out.println("Succeeded connecting to the Hive Database!");
    	
	    	return conn;*/
		String url = "jdbc:hive://119.161.183.203:10006/default";
		String db_driver = "org.apache.hadoop.hive.jdbc.HiveDriver";
		String user = "";
		String password = "";
    
	    	Properties prop = new Properties();
	    	prop.put("user", user);
	    	prop.put("password",password);
	    	prop.put("charSet","UTF-8");
	
	    	Class.forName(db_driver);	    
	
	    	Connection conn = DriverManager.getConnection(url, prop);
	    	
	    	if(!conn.isClosed()) 
	    	{	
	    		//System.out.println("Succeeded connecting to the Hive Database!");
	    	}else 
	    	{
	    		System.out.println("Connect Hive Failed!");
	    		return null;
	    	}
	    	return conn;
	}
}
