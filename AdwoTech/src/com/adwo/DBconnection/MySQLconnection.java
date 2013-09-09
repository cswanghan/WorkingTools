package com.adwo.DBconnection;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.log4j.Logger;


public class MySQLconnection {
	
	private static final Logger logger = Logger.getLogger(MySQLconnection.class);
	
	public static Connection getConnection(String propertyfile) throws Exception
	{
			Properties prop = new Properties();  
			//FileInputStream fis = new FileInputStream("externalsrc/db.properties");  
			FileInputStream fis = new FileInputStream(propertyfile);
			prop.load(fis);  
			String driver = prop.getProperty("mysql_driver");
			Class.forName(driver);
			
			String URL = prop.getProperty("mysql_url");
			String username = prop.getProperty("mysql_username");
			String password = prop.getProperty("mysql_password");
			
			
			Connection conn = DriverManager.getConnection(URL, username, password);
			if(!conn.isClosed()) 
			{
				logger.info("Succeeded connecting to the MySQL Database!");
			}
			else 
				logger.error("Failed connecting to the MySQL Database!");
			
			return conn;
	}
	
	
}
