/**
 * 
 */
package com.adwo.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

import com.adwo.DBconnection.HiveConnection;
import com.adwo.DBconnection.MySQLconnection;

/**
 * @author dev
 *
 */
public class UpdateValidateClick {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		UpdateValidateClick uvc = new UpdateValidateClick();
		//uvc.CalculateValidateClick("2013-04-02");
		//uvc.CalculateValidateClick("2013-05-01");
		//uvc.CalculateValidateClick("2013-05-02");
		String propertyfile = "externalsrc/db.properties";
		String date = null;
		for(int i = 1; i <= 30 ; i++)
		{
			if(i < 10)
			{
				date = "2013-02-0" + i;
				System.out.println("Currently inserting : " + date);
				uvc.CalculateValidateClick(date,propertyfile);
			}
			else
			{	
				date = "2013-02-" + i;
				System.out.println("Currently inserting : " + date);
				uvc.CalculateValidateClick(date,propertyfile);
			}
		}
	}

	public void CalculateValidateClick(String date, String prooertyfile) throws Exception
	{
		HashMap<String, Integer> ClickMap = new HashMap<String, Integer>();
		String query1 = "select app_id, ad_id, count(udid) from ios_click_log where nothing2 = 1 and dt = '" + date + "' group by app_id, ad_id";
		String query2 = "select app_id, ad_id, count(udid) from ios_click_log2 where nothing2 = 1 and dt = '" + date + "' group by app_id, ad_id";
		Connection conn = HiveConnection.getConnection();
		Statement stmt = conn.createStatement();
		System.out.println("Executing : " + query1);
		ResultSet rs = stmt.executeQuery(query1);
		while(rs.next())
		{
			String app_id = rs.getString(1);
			String ad_id = rs.getString(2);
			int validateclick = rs.getInt(3);
			String combinedkey = app_id + "-" + ad_id;
			if(!ClickMap.containsKey(combinedkey))
			{
				ClickMap.put(combinedkey, validateclick);
			}
			else
			{
				int temp_count = ClickMap.get(combinedkey);
				temp_count = temp_count + validateclick;
				ClickMap.put(combinedkey, temp_count);
			}
		}
		rs.close();
		System.out.println("Executing : " + query2);
		ResultSet rs2 = stmt.executeQuery(query2);
		while(rs2.next())
		{
			String app_id = rs2.getString(1);
			String ad_id = rs2.getString(2);
			int validateclick = rs2.getInt(3);
			String combinedkey = app_id + "-" + ad_id;
			if(!ClickMap.containsKey(combinedkey))
			{
				ClickMap.put(combinedkey, validateclick);
			}
			else
			{
				int temp_count = ClickMap.get(combinedkey);
				temp_count = temp_count + validateclick;
				ClickMap.put(combinedkey, temp_count);
			}
		}
		rs2.close();
				
		InsertIntoDB(ClickMap,date,prooertyfile);
	}
	
	public void InsertIntoDB(HashMap<String, Integer> ClickMap, String Date, String propertyfile) throws Exception
	{
		System.out.println("Inserting to 226DB");
		Connection conn = MySQLconnection.getConnection(propertyfile);
		
		Iterator<String> map_iter = ClickMap.keySet().iterator();
		while(map_iter.hasNext())
		{
			String key = map_iter.next();
			String[] details = key.split("-");
			String app_id = details[0];
			String ad_id = details[1];
			int click = ClickMap.get(key);
			
			PreparedStatement preStmt=conn.prepareStatement("INSERT INTO report_adv_prg_update_validate_click"+
					"(advid, prgid, createtime, validate_click)"+
					"values(?,?,?,?)");
			conn.setAutoCommit(false);
			
			preStmt.setInt(1, Integer.parseInt(ad_id));
			preStmt.setInt(2, Integer.parseInt(app_id));
			preStmt.setString(3, Date);
			preStmt.setInt(4, click);
			
			int j = preStmt.executeUpdate();
			conn.commit();
			if(j!=0)
				//System.out.println("Insert/OR/Update local db done!");
			preStmt.close();
			
		}
		conn.close();
	}
}
