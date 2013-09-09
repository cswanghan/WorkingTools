package com.adwo.UserInfo;

import java.io.FileInputStream;
import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.PreparedStatement;
import java.sql.ResultSet;  
import java.sql.SQLException;  
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class GenerateUserInfo {

//	连接数据库
	public static Connection getConnection(String propertyfile) throws Exception
	{
			Properties prop = new Properties();  
			//FileInputStream fis = new FileInputStream("externalsrc/db.properties");  // 数据库配置文件
			FileInputStream fis = new FileInputStream(propertyfile);  // 数据库配置文件
			prop.load(fis);
			
			String driver = prop.getProperty("db_driver");
			Class.forName(driver);
			
			String URL = prop.getProperty("url");
			String username = prop.getProperty("username");
			String password = prop.getProperty("password");
			
			
			Connection conn = DriverManager.getConnection(URL, username, password);
			if(!conn.isClosed()) 
			{
				System.out.println("Succeeded connecting to the Database!");
			}
			else 
				System.out.println("Failed connecting to the Database!");
			
			return conn;
	}
	
//	从Hive数据库查询udid并插入Mysql数据库的adv_udid表中
	public static void getUdid(String adlist, String begindate, String enddate, String platform, String hivepropertyfile, String mysqlpropertyfile) throws Exception{	

		String log = null;
		
		if(platform.equals("2"))
			log = "click_log";
		else if(platform.equals("3"))
			log = "ios_click_log";
		else if(platform.equals("1"))
			log = "ios_fsclick_log";
		else
		{
			System.out.println("Invalid platform id!");
			return;
		}
		
		Date date = new Date();
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
		String rundate = dateformat.format(date);
		
		Connection hiveconn = getConnection(hivepropertyfile);	// 建立Hive数据库连接  
        	
    	String hivesql = "select ad_id, udid from " + log + " where ad_id in (" + adlist + ") and dt >= '" + begindate
    			+ "' and dt <= '" + enddate + "' group by ad_id, udid";  // 查询数据
          
    	Statement hivest = hiveconn.createStatement();    // 创建用于执行静态sql语句的Statement对象  
        
        ResultSet hivers = hivest.executeQuery(hivesql);      // 执行sql查询语句，返回查询数据的结果集  
        
        hivest.close();
        
        Connection conn = getConnection(mysqlpropertyfile);	// 建立Mysql数据库连接  
    	
    	String sql = "insert ignore into adv_udid (createtime, advid, udid) values (?,?,?)";
          
    	PreparedStatement pst = conn.prepareStatement(sql);    
        
        while (hivers.next()) {	// 判断是否还有下一个数据  
        	
        	conn.setAutoCommit(false);
        	pst.setString(1, rundate);
        	pst.setInt(2, hivers.getInt("ad_id"));
        	pst.setString(3, hivers.getString("udid"));
            // 把一个SQL命令加入命令列表  
            pst.executeUpdate(); 
            conn.commit();
        }
        
        // 执行批量更新  
        //pst.executeBatch();
        pst.close();
        hiveconn.close();   //关闭数据库连接
        conn.close();   //关闭数据库连接
        
    }
	
//	统计用户信息并插入Mysql数据库的adv_usersta表中
	public static void countUserInfo(String adlist) throws Exception{	
		
		Date date = new Date();
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
		String rundate = dateformat.format(date);
		
		String[] ads = adlist.split(",");
		
		Connection conn = getConnection("externalsrc/mysql.properties");	// 建立Mysql数据库连接   
		
		for (int i = 0; i < ads.length; i++)
		{
			String sql = "select b.answer from adv_udid a join adwo_questionnaire b on b.equipmentID = a.udid where " +
					"date(b.createtime) >='2013-01-01' and date(b.createtime) <='2013-06-31' and a.udid is not null and a.advid = "
					+ ads[i] + " and a.createtime = '" + rundate + "'";
			
			Statement st = conn.createStatement();    // 创建用于执行静态sql语句的Statement对象  
			
			ResultSet rs = st.executeQuery(sql);      // 执行sql查询语句，返回查询数据的结果集 
			
			Map<String, Integer> ansmap = new HashMap<String, Integer>();
			
			while (rs.next()) {	// 判断是否还有下一个数据  
	        	
	        	String[] answers = rs.getString("answer").split("\\|");
	        	for (int j = 0; j < 5; j++)
	        	{
	        		if(ansmap.containsKey(answers[j]))
	        			ansmap.put(answers[j], ansmap.get(answers[j])+1);
	        		else
	        			ansmap.put(answers[j], 1);
	        	}
	        }
			
			PreparedStatement pst = conn.prepareStatement("replace into adv_usersta (createtime, advid, questionid, answer, count)"+
					"values(?,?,?,?,?)");
			
			Iterator it = ansmap.entrySet().iterator();
			while (it.hasNext()) {	// 判断是否还有下一个数据  
				
				Map.Entry entry = (Map.Entry) it.next(); 
				String key =  (String) entry.getKey();
				int value = Integer.valueOf(entry.getValue().toString());
				String[] questionanswer = key.split(":");
		        	
				pst.setString(1, rundate);
				pst.setInt(2, Integer.parseInt(ads[i]));
				pst.setInt(3, Integer.parseInt(questionanswer[0]));
				pst.setString(4, questionanswer[1]);
				pst.setInt(5, value);
				//pst.setInt(6, value);
				// 把一个SQL命令加入命令列表  
				pst.executeUpdate();
			}
			// 执行批量更新  
			//pst.executeBatch();	
		}
        
        conn.close();   //关闭数据库连接
        
    }
	
	public static void main(String[] args) throws Exception {
		//args[0] = ad list
		//args[1] = begin date
		//args[2] = end date
		//args[3] = platform type
		//args[4] = db.propertyfile
		//getUdid("7279,7278", "2013-08-30", "2013-09-02", "1");
		//countUserInfo("7279,7278");
		getUdid(args[0], args[1], args[2], args[3], args[4], args[5]);
		countUserInfo(args[0]);
		System.out.println("程序运行结束！");
	}
	
}
