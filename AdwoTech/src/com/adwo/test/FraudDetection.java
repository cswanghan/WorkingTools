/**
 * 
 */
package com.adwo.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

import com.adwo.DBconnection.HiveConnection;

/**
 * @author dev
 *
 */
public class FraudDetection {

	
	HashMap<String, Integer> Udid_Count_Map_1 = new HashMap<String, Integer>();
	HashMap<String, Integer> Udid_Count_Map_2 = new HashMap<String, Integer>();
	HashMap<String, Integer> Udid_Count_Map_all = new HashMap<String, Integer>();
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		FraudDetection fd = new FraudDetection();
		try {
			//fd.LoadDataFromHive("1", "50", "2013-05-09");
			if(args.length < 3)
				System.out.println("Wrong command line. Correct Format : java -jar FraudDetection [flag] [threshold] [date]");
			else if(args.length >= 4)
				System.out.println("Wrong command line. Correct Format : java -jar FraudDetection [flag] [threshold] [date]");
			else
				fd.LoadDataFromHive(args[0],args[1],args[2]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void init()
	{
		Udid_Count_Map_1.clear();
		Udid_Count_Map_2.clear();
		Udid_Count_Map_all.clear();
	}

	public void LoadDataFromHive(String flag_str, String threshold_str, String date) throws Exception
	{
		int flag = Integer.parseInt(flag_str);
		int threshold = Integer.parseInt(threshold_str);
		System.out.println("Threshold = " + threshold);
		if(flag == 1)		//ios
		{
			init();
			StringBuffer query1 = new StringBuffer();
			query1.append("select concat(app_id, ' @ ', udid) from ios_click_log where dt = '").append(date).append("'");
			StringBuffer query2 = new StringBuffer();
			query2.append("select concat(app_id, ' @ ', udid) from ios_click_log2 where dt = '").append(date).append("'");
			Connection conn = HiveConnection.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query1.toString());
			while(rs.next())
			{
				String combinedkey = rs.getString(1);
				if(!Udid_Count_Map_1.containsKey(combinedkey))
				{
					Udid_Count_Map_1.put(combinedkey, 1);
				}
				else
				{
					int temp_count = Udid_Count_Map_1.get(combinedkey);
					temp_count++;
					Udid_Count_Map_1.put(combinedkey, temp_count);
				}
			}
			rs.close();
			
			ResultSet rs2 = stmt.executeQuery(query2.toString());
			while(rs2.next())
			{
				String combinedkey = rs2.getString(1);
				if(!Udid_Count_Map_2.containsKey(combinedkey))
				{
					Udid_Count_Map_2.put(combinedkey, 1);
				}
				else
				{
					int temp_count = Udid_Count_Map_2.get(combinedkey);
					temp_count++;
					Udid_Count_Map_2.put(combinedkey, temp_count);
				}
			}
			rs2.close();
			stmt.close();
						
			Iterator<String> map_iter_1 = Udid_Count_Map_1.keySet().iterator();
			while(map_iter_1.hasNext())
			{
				String key = map_iter_1.next();
				Udid_Count_Map_all.put(key, Udid_Count_Map_1.get(key));
			}
			
			Iterator<String> map_iter_2 = Udid_Count_Map_2.keySet().iterator();
			while(map_iter_2.hasNext())
			{
				String key = map_iter_2.next();
				if(!Udid_Count_Map_all.containsKey(key))
				{
					Udid_Count_Map_all.put(key, Udid_Count_Map_2.get(key));
				}
				else
				{
					int temp_count = Udid_Count_Map_all.get(key);
					temp_count = temp_count + Udid_Count_Map_2.get(key);
					Udid_Count_Map_all.put(key, temp_count);
				}
			}
			
			Iterator<String> map_print_iter	= Udid_Count_Map_all.keySet().iterator();
			while(map_print_iter.hasNext())
			{
				String key = map_print_iter.next();
				if(Udid_Count_Map_all.get(key) >= threshold)
					System.out.println(key + "\t" + Udid_Count_Map_all.get(key));
			}
			//Combined two hashmap and save result in the first map
		}
		else			//android
		{
			init();
			StringBuffer query1 = new StringBuffer();
			query1.append("select concat(app_id, ' @ ', udid) from click_log where dt = '").append(date).append("'");
			StringBuffer query2 = new StringBuffer();
			query2.append("select concat(app_id, ' @ ', udid) from click_log2 where dt = '").append(date).append("'");
			Connection conn = HiveConnection.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query1.toString());
			while(rs.next())
			{
				String combinedkey = rs.getString(1);
				if(!Udid_Count_Map_1.containsKey(combinedkey))
				{
					Udid_Count_Map_1.put(combinedkey, 1);
				}
				else
				{
					int temp_count = Udid_Count_Map_1.get(combinedkey);
					temp_count++;
					Udid_Count_Map_1.put(combinedkey, temp_count);
				}
			}
			rs.close();
			
			ResultSet rs2 = stmt.executeQuery(query2.toString());
			while(rs2.next())
			{
				String combinedkey = rs2.getString(1);
				if(!Udid_Count_Map_2.containsKey(combinedkey))
				{
					Udid_Count_Map_2.put(combinedkey, 1);
				}
				else
				{
					int temp_count = Udid_Count_Map_2.get(combinedkey);
					temp_count++;
					Udid_Count_Map_2.put(combinedkey, temp_count);
				}
			}
			rs2.close();
			stmt.close();
						
			Iterator<String> map_iter_1 = Udid_Count_Map_1.keySet().iterator();
			while(map_iter_1.hasNext())
			{
				String key = map_iter_1.next();
				Udid_Count_Map_all.put(key, Udid_Count_Map_1.get(key));
			}
			
			Iterator<String> map_iter_2 = Udid_Count_Map_2.keySet().iterator();
			while(map_iter_2.hasNext())
			{
				String key = map_iter_2.next();
				if(!Udid_Count_Map_all.containsKey(key))
				{
					Udid_Count_Map_all.put(key, Udid_Count_Map_2.get(key));
				}
				else
				{
					int temp_count = Udid_Count_Map_all.get(key);
					temp_count = temp_count + Udid_Count_Map_2.get(key);
					Udid_Count_Map_all.put(key, temp_count);
				}
			}
			
			Iterator<String> map_print_iter	= Udid_Count_Map_all.keySet().iterator();
			while(map_print_iter.hasNext())
			{
				String key = map_print_iter.next();
				if(Udid_Count_Map_all.get(key) >= threshold)
					System.out.println(key + "\t" + Udid_Count_Map_all.get(key));
			}
		}
	}
}
