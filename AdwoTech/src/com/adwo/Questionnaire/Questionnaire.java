package com.adwo.Questionnaire;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.adwo.DBconnection.HiveConnection;
import com.adwo.DBconnection.MySQLconnection;


public class Questionnaire extends QuestionnaireBody{

	public static List<String> UserList = new ArrayList<String>();
	public static HashMap<String, String> User_Answer_Map = new HashMap<String, String>();
	
	public static void GetPeriodAnswerFromMySql(String startdate, String enddate) 
	{
		try {
			Connection conn = MySQLconnection.getConnection("externalsrc/db.properties");
			Statement stmt = conn.createStatement();
			String select_answer = "select answer, equipmentId from adwo_questionnaire where createtime >='" + startdate + "' and createtime <='" + enddate + "'";
			ResultSet rs = stmt.executeQuery(select_answer);
			while(rs.next())
			{
				String answer = rs.getString(1);
				String[] details = answer.split("[:\\|]");
				StringBuffer part_answer = new StringBuffer(details[1]);
				part_answer.append(",").append(details[3]).append(",").append(details[5]).append(",").append(details[7]).append(",").append(details[9]);
				String udid = rs.getString(2);
				if(!UserList.contains(udid))
				{
					UserList.add(udid);
					User_Answer_Map.put(udid, part_answer.toString());
				}
				System.out.println(answer + "|||" + part_answer.toString() + "," + udid);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void GetPeriodUserAppActivityFromHive(String startdate, String enddate) 
	{
		/*
		 * Date: 	2013-02-21
		 * Author:	Han Wang @ Adwo
		 * 
		 * Description:
		 * 		This function will get user App Activity based on input date and udid
		 */
		String period_udid = ListToString(UserList);
		
		String Query1 = "select app_id, udid from ios_request_log where udid in (" + period_udid + ") and dt >='" + startdate + "' and dt <='" + enddate + "'";
		String Query2 = "select app_id, udid from ios_request_log2 where udid in (" + period_udid + ") and dt >='" + startdate + "' and dt <='" + enddate + "'";
		String Query3 = "select app_id, udid from android_request_log where udid in (" + period_udid + ") and dt >='" + startdate + "' and dt <='" + enddate + "'";
		String Query4 = "select app_id, udid from android_request_log2 where udid in (" + period_udid + ") and dt >='" + startdate + "' and dt <='" + enddate + "'";
		
		StringBuffer Total_query = new StringBuffer();
		//Total_query.append(Query1).append(" union all ").append(Query2);
		//.append(" union all ").append(Query3).append(" union all ").append(Query4);
		System.out.println(Query1);
		
		try {
			Connection conn = HiveConnection.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(Query1);
			while(rs.next())
			{
				String appid = rs.getString(1);
				String udid = rs.getString(2);
				String answer = User_Answer_Map.get(udid);
				System.out.println(udid + ":" + appid + ":" + answer);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static String ListToString(List<String> InputList)
	{
		StringBuffer temp = new StringBuffer();
		Iterator<String> list_iter = InputList.iterator();
		int count = 0;	
		while(list_iter.hasNext())
		{
			Object key = list_iter.next();
			if(count == 0)
				temp.append("'").append(key.toString()).append("'");
			else
				temp.append(",").append("'").append(key.toString()).append("'");
			count++;
		}
		return temp.toString();
	}
}
