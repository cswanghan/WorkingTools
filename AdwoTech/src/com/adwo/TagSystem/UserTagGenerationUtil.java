/**
 * 
 */
package com.adwo.TagSystem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import com.adwo.DBconnection.HiveConnection;
import com.adwo.DBconnection.MySQLconnection;

/**
 * @author dev
 *
 */
public class UserTagGenerationUtil {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		UserTagGenerationUtil utg = new UserTagGenerationUtil();
		try {
			utg.FileReader(0,1,"2013-01-02","externalsrc/db.properties");
			//utg.FileReader(Integer.valueOf(args[0]), Integer.valueOf(args[1]), args[2], args[3]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * @param inputfile
	 * @param type_flag = 1:click | 2:show
	 * 
	 * @e.x. input example: udid\tcount
	 */
	public void FileReader(int platform_id, int type_flag, String date, String db_file) throws Exception
	{
		Connection conn = HiveConnection.getConnection();
		String query = null;
		int actionid = 0;
		if(platform_id == 0) //ios
		{
			switch(type_flag){
				case 1: //click
				{
				
					query = "select udid, ad_id, count(*) from ios_click_log where dt = '" + date + "' group by udid, ad_id";
					actionid = 1;
					break;
				}
				case 2: //show
				{
					query = "select udid, ad_id, count(*) from ios_show_log where dt = '" + date + "' group by udid, ad_id";
					actionid = 2;	
					break;
				}
			}
		}
		else //android
		{
			switch(type_flag){
				case 1: //click
				{
					query = "select udid, ad_id, count(*) from click_log where dt = '" + date + "' group by udid, ad_id";
					actionid = 1;
					break;
				}
				case 2: //show
				{
					query = "select udid, ad_id, count(*) from show_log where dt = '" + date + "' group by udid, ad_id";
					actionid = 2;
					break;
				}
			}
		}
		
		Connection mysql_conn = MySQLconnection.getConnection(db_file);
		
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		String udid = null, tagid = null, action_count = null;
		int count = 0; 
		PreparedStatement preStmt = null;
		java.util.Date dt=new java.util.Date();
		Timestamp tt = new Timestamp(dt.getTime());
		preStmt = mysql_conn.prepareStatement("INSERT INTO tag_sys_actioninfo"+
				"(udid, tagid, appeardt, updatetime, ActionType, ActionCount)"+
				"values(?,?,?,?,?,?) on duplicate key update ActionCount = ActionCount + ?, UpdateTime = NOW()");
		
		while(rs.next())
		{
			try{
				udid = rs.getString(1);
				tagid = rs.getString(2);
				action_count = rs.getString(3);
				if(udid.contains("?"))
				{
					System.err.println(count + ":" + udid + ";" + tagid + ";" + action_count);
					continue;
				}
				else
				{
//					java.util.Date dt=new java.util.Date();
//					Timestamp tt = new Timestamp(dt.getTime());
//					preStmt = mysql_conn.prepareStatement("INSERT INTO tag_sys_actioninfo"+
//							"(udid, tagid, appeardt, updatetime, ActionType, ActionCount)"+
//							"values(?,?,?,?,?) on duplicate key update ActionCount = ActionCount + '" + action_count + "', UpdateTime = '" + tt + "'");
//					
					mysql_conn.setAutoCommit(false);
					
					preStmt.setString(1, udid);
					preStmt.setInt(2, Integer.parseInt(tagid));
					preStmt.setString(3, date);
					preStmt.setTimestamp(4, tt);
					preStmt.setInt(5, actionid);
					preStmt.setString(6, action_count);
					preStmt.setString(7, action_count);
					preStmt.addBatch();
					
					if(count > 50000)
					{
						count = 0;
						preStmt.executeBatch();
						mysql_conn.commit();
						System.out.println("Batch Updated;");
					}
					/*int j = preStmt.executeUpdate();
					mysql_conn.commit();
					if(j!=0)
						//System.out.println("Insert/OR/Update local db done!");
					*/
					count++;
				}
			}catch(Exception e)
			{
				e.printStackTrace();
				System.err.println(count + ":" + udid + ";" + tagid + ";" + action_count);
			}
		}
		preStmt.executeBatch();
		mysql_conn.commit();
		preStmt.executeBatch();
		mysql_conn.commit();
		preStmt.close();
		conn.close();
		mysql_conn.close();
		System.out.println("done!");
	}
}
