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

//	�������ݿ�
	public static Connection getConnection(String propertyfile) throws Exception
	{
			Properties prop = new Properties();  
			//FileInputStream fis = new FileInputStream("externalsrc/db.properties");  // ���ݿ������ļ�
			FileInputStream fis = new FileInputStream(propertyfile);  // ���ݿ������ļ�
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
	
//	��Hive���ݿ��ѯudid������Mysql���ݿ��adv_udid����
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
		
		Connection hiveconn = getConnection(hivepropertyfile);	// ����Hive���ݿ�����  
        	
    	String hivesql = "select ad_id, udid from " + log + " where ad_id in (" + adlist + ") and dt >= '" + begindate
    			+ "' and dt <= '" + enddate + "' group by ad_id, udid";  // ��ѯ����
          
    	Statement hivest = hiveconn.createStatement();    // ��������ִ�о�̬sql����Statement����  
        
        ResultSet hivers = hivest.executeQuery(hivesql);      // ִ��sql��ѯ��䣬���ز�ѯ���ݵĽ����  
        
        hivest.close();
        
        Connection conn = getConnection(mysqlpropertyfile);	// ����Mysql���ݿ�����  
    	
    	String sql = "insert ignore into adv_udid (createtime, advid, udid) values (?,?,?)";
          
    	PreparedStatement pst = conn.prepareStatement(sql);    
        
        while (hivers.next()) {	// �ж��Ƿ�����һ������  
        	
        	conn.setAutoCommit(false);
        	pst.setString(1, rundate);
        	pst.setInt(2, hivers.getInt("ad_id"));
        	pst.setString(3, hivers.getString("udid"));
            // ��һ��SQL������������б�  
            pst.executeUpdate(); 
            conn.commit();
        }
        
        // ִ����������  
        //pst.executeBatch();
        pst.close();
        hiveconn.close();   //�ر����ݿ�����
        conn.close();   //�ر����ݿ�����
        
    }
	
//	ͳ���û���Ϣ������Mysql���ݿ��adv_usersta����
	public static void countUserInfo(String adlist) throws Exception{	
		
		Date date = new Date();
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
		String rundate = dateformat.format(date);
		
		String[] ads = adlist.split(",");
		
		Connection conn = getConnection("externalsrc/mysql.properties");	// ����Mysql���ݿ�����   
		
		for (int i = 0; i < ads.length; i++)
		{
			String sql = "select b.answer from adv_udid a join adwo_questionnaire b on b.equipmentID = a.udid where " +
					"date(b.createtime) >='2013-01-01' and date(b.createtime) <='2013-06-31' and a.udid is not null and a.advid = "
					+ ads[i] + " and a.createtime = '" + rundate + "'";
			
			Statement st = conn.createStatement();    // ��������ִ�о�̬sql����Statement����  
			
			ResultSet rs = st.executeQuery(sql);      // ִ��sql��ѯ��䣬���ز�ѯ���ݵĽ���� 
			
			Map<String, Integer> ansmap = new HashMap<String, Integer>();
			
			while (rs.next()) {	// �ж��Ƿ�����һ������  
	        	
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
			while (it.hasNext()) {	// �ж��Ƿ�����һ������  
				
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
				// ��һ��SQL������������б�  
				pst.executeUpdate();
			}
			// ִ����������  
			//pst.executeBatch();	
		}
        
        conn.close();   //�ر����ݿ�����
        
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
		System.out.println("�������н�����");
	}
	
}
