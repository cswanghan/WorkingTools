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
public class UserTag {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		UserTag utg = new UserTag();
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
				
					query = "select udid, idfa, mmc_mnc,network, province_id from ios_click_log where dt = '" + date + "' and length(idfa) > 4 group by udid, idfa, mmc_mnc,network, province_id, ad_id";
					actionid = 1;
					break;
				}
				case 2: //show
				{
					query = "select udid, device_code, mmc_mnc,network, province_id from ios_show_log where dt = '" + date + "' and length(device_code) > 4 group by udid, device_code, mmc_mnc,network, province_id, ad_id";
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
		String udid = null, idfa = null, operator = null, network = null, osversion = null, area = null;
		int count = 0; 
		PreparedStatement preStmt = null;
		java.util.Date dt=new java.util.Date();
		Timestamp tt = new Timestamp(dt.getTime());
		preStmt = mysql_conn.prepareStatement("INSERT INTO user_info_ios"+
				"(udid, idfa, operator, network, osversion, area)"+
				"values(?,?,?,?,?,?)");
		
		while(rs.next())
		{
			try{
				udid = rs.getString(1);
				idfa = rs.getString(2);
				operator = rs.getString(3);
				network = rs.getString(4);
				area = rs.getString(5);
				if(udid.contains("?"))
				{
					System.err.println(count + ":" + udid + ";" + idfa + ";" + operator);
					continue;
				}
				else
				{
					mysql_conn.setAutoCommit(false);

					preStmt.setString(1, udid);
					preStmt.setString(2, idfa);
					preStmt.setString(3, operator);
					preStmt.setString(4, network);
					preStmt.setString(5, "");
					preStmt.setString(6, area);
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
				System.err.println(count + ":" + udid + ";" + idfa + ";" + operator);
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
