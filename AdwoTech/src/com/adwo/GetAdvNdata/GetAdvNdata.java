/**
 * 
 */
package com.adwo.GetAdvNdata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Pattern;

import com.adwo.DBconnection.HiveConnection;
import com.adwo.DBconnection.MySQLconnection;

/**
 * @author 	Han Wang
 * @date   	2013-07-02
 * @description This function will calculate advNdata everyday
 *
 */
public class GetAdvNdata {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		GetAdvNdata gan = new GetAdvNdata();
		gan.GetAdNdata("2013-08-19");
		//gan.GetAdNdata(args[0]);
	}
	
	/**
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isNumeric(String str)
	{
		    Pattern pattern = Pattern.compile("[0-9]*");
		    return pattern.matcher(str).matches();   
	}
	
	/**
	 * 
	 * @param Date
	 */
	public void GetAdNdata(String Date)
	{
		HashMap<String, Integer> Adv_PUV_Count_Map = new HashMap<String, Integer>();
		HashMap<String, Integer> Adv_CUV_Count_Map = new HashMap<String, Integer>();
		Connection conn;
		try
		{
			conn = HiveConnection.getConnection();
			Statement stmt = conn.createStatement();
			String priority = "set mapred.job.priority = HIGH";
			stmt.execute(priority);
			stmt.execute("set hive.exec.parallel = true");
			
			String select_query = "select ad_id, count, count(udid) from adv_n_show_data where ad_id is not null and count is not null group by ad_id, count";
			System.out.println("Executing " + select_query);
			ResultSet rs = stmt.executeQuery(select_query);
			while(rs.next())
			{
					String ad_id = rs.getString(1);
					String num = rs.getString(2);
					int count = Integer.parseInt(rs.getString(3));
					String combinedkey = ad_id + "," + num;
					if(!Adv_PUV_Count_Map.containsKey(combinedkey))
						Adv_PUV_Count_Map.put(combinedkey, count);
					else
					{
						int temp_count = Adv_PUV_Count_Map.get(combinedkey);
						temp_count++;
						Adv_PUV_Count_Map.put(combinedkey, temp_count);
					}
			}
			rs.close();
			
			select_query = "select ad_id, count, count(udid) from adv_n_click_data where ad_id is not null and count is not null group by ad_id, count";
			System.out.println("Executing " + select_query);
			ResultSet rs2 = stmt.executeQuery(select_query);
			while(rs2.next())
			{
					String ad_id = rs2.getString(1);
					String num = rs2.getString(2);
					int count = rs2.getInt(3);
					String combinedkey = ad_id + "," + num;
					if(!Adv_CUV_Count_Map.containsKey(combinedkey))
						Adv_CUV_Count_Map.put(combinedkey, count);
					else
					{
						int temp_count = Adv_CUV_Count_Map.get(combinedkey);
						temp_count++;
						Adv_CUV_Count_Map.put(combinedkey, temp_count);
					}
			}
			rs2.close();
			stmt.close();
			
			java.sql.Connection mysql_conn = MySQLconnection.getConnection("externalsrc/db.properties");		
			
			System.out.println("Calculating PV N+ data...");
			StringBuilder resultData = new StringBuilder("showresultData=");
			Iterator<String> puv_iter = Adv_PUV_Count_Map.keySet().iterator();
			int count = 0;
			while(puv_iter.hasNext())
			{
				Object key = puv_iter.next();
				if(count == 0)
					resultData.append(key).append(",").append(Date).append(",").append(Adv_PUV_Count_Map.get(key));
				else
					resultData.append("|").append(key).append(",").append(Date).append(",").append(Adv_PUV_Count_Map.get(key));
				count++;
				
				String[] key_details = key.toString().split(",");
				String advid = key_details[0];
				String show_count = key_details[1];
				
				if(show_count == null || show_count.length() < 0)
					continue;
				else if(isNumeric(advid))
				{
					PreparedStatement preStmt = mysql_conn.prepareStatement("INSERT INTO adv_show_n "+ 
							" (advid,show_count,createtime,num)"+
							"values(?,?,?,?)");
					mysql_conn.setAutoCommit(false);
					//System.out.println(key.toString());
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.valueOf(show_count));
					preStmt.setString(3, Date);
					preStmt.setInt(4, Adv_PUV_Count_Map.get(key));
					int j = preStmt.executeUpdate();
					mysql_conn.commit();
					if(j!=0)
						//System.out.println("Insert/OR/Update local db done!");
					preStmt.close();
				}
			}
			System.out.println(resultData.toString());
			
			count = 0;
			System.out.println("Calculating CV N+ data...");
			resultData = new StringBuilder("clickresultData=");
			Iterator<String> cuv_iter = Adv_CUV_Count_Map.keySet().iterator();
			while(cuv_iter.hasNext())
			{
				Object key = cuv_iter.next();
				if(count == 0)
					resultData.append(key).append(",").append(Date).append(",").append(Adv_CUV_Count_Map.get(key));
				else
					resultData.append("|").append(key).append(",").append(Date).append(",").append(Adv_CUV_Count_Map.get(key));
				count++;
				
				String[] key_details = key.toString().split(",");
				String advid = key_details[0];
				String show_count = key_details[1];
				
				if(show_count == null || show_count.length() < 0)
					continue;
				else if(isNumeric(advid))
				{
					PreparedStatement preStmt=mysql_conn.prepareStatement("INSERT INTO adv_click_n "+ 
							" (advid,click_count,createtime,num)"+
							"values(?,?,?,?)");
					mysql_conn.setAutoCommit(false);
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.valueOf(show_count));
					preStmt.setString(3, Date);
					preStmt.setInt(4, Adv_CUV_Count_Map.get(key));
					int j = preStmt.executeUpdate();
					mysql_conn.commit();
					if(j!=0)
						//System.out.println("Insert/OR/Update local db done!");
					preStmt.close();
				}
			}
			System.out.println(resultData.toString());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
