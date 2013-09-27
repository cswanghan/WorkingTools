/**
 * 
 */
package com.adwo.GetAndroidEffect;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

import com.adwo.DBconnection.HiveConnection;

/**
 * @author HanWang
 *
 */
public class GetAndroidEffect {

	
	public void GetEffect(String clickdate, String effectdate, int flag)
	{
		try{
			Connection hive_conn = HiveConnection.getConnection();
			String query_sql = "select concat(b.dt,' ',b.hour,':',b.time_minute,'.',b.time_second),b.* from adv_blacklist a join click_log b on a.advid = b.ad_id and a.imei = b.udid where b.dt >= '" + clickdate + "' and b.dt <= '" + effectdate + "' and a.createtime like '%" + effectdate + "%' order by concat(b.dt,' ',b.hour,':',b.time_minute,'.',b.time_second) asc";
			Statement stmt = hive_conn.createStatement();
			ResultSet rs = stmt.executeQuery(query_sql);
			HashMap<String, String> result_map = new HashMap<String, String>();
			HashMap<String, Integer> count_map = new HashMap<String, Integer>();
			int count = 0;
			while(rs.next())
			{
				StringBuilder sb = new StringBuilder();
				String time_minute = rs.getString(2);
				String time_second = rs.getString(3);
				String platform_id = rs.getString(4);
				String ad_id = rs.getString(5);
				String req_ip = rs.getString(6);
				String app_id = rs.getString(7);
				String udid = rs.getString(8);
				String sdk_v = rs.getString(9);
				String nothing = rs.getString(10);
				String device_id = rs.getString(11);
				String mnc = rs.getString(12);
				String network = rs.getString(13);
				String jailbreak = rs.getString(14);
				String province_id = rs.getString(15);
				String dt = rs.getString(16);
				String hour = rs.getString(17);
				sb.append(time_minute).append(",").append(time_second).append(",").append(platform_id).append(",")
				.append(ad_id).append(",").append(req_ip).append(",").append(app_id).append(",").append(udid).append(",")
				.append(sdk_v).append(",").append(nothing).append(",").append(device_id).append(",").append(mnc).append(",")
				.append(network).append(",").append(jailbreak).append(",").append(province_id).append(",")
				.append(dt).append(",").append(hour);
				
				
				
				if(flag == 0)
				{
					String combinedkey = udid + "-" + ad_id + "-" + app_id;
					result_map.put(combinedkey, sb.toString());
				}
				else
				{
					String combinedkey = udid + "-" + ad_id + "-" + app_id;
					if(!count_map.containsKey(combinedkey))
						count_map.put(combinedkey, 1);
					else
					{
						int temp = count_map.get(combinedkey);
						temp++;
						count_map.put(combinedkey, temp);
					}
				}
				count++;
			}
			
			if(flag == 0)
			{
				Iterator<String> map_iter = result_map.keySet().iterator();
				while(map_iter.hasNext())
				{
					String key = map_iter.next();
					System.out.println(result_map.get(key));
				}
			}
			else
			{
				Iterator<String> map_iter = count_map.keySet().iterator();
				while(map_iter.hasNext())
				{
					String key = map_iter.next();
					System.out.println(key + "||" + count_map.get(key));
				}
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		/**
		 * @param args
		 */
		// TODO Auto-generated method stub
		GetAndroidEffect ge = new GetAndroidEffect();
		ge.GetEffect(args[0], args[1], Integer.valueOf(args[2]));
	}

}
