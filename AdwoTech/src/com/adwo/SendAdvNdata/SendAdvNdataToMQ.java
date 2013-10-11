/**
 * 
 */
package com.adwo.SendAdvNdata;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.adwo.DBconnection.MySQLconnection;

/**
 * @author dev
 *
 */
public class SendAdvNdataToMQ implements SendDataInterface{
	
	private HashMap<String, String> result_map = new HashMap<String, String>();
	private HashMap<String, String> uv_map = new HashMap<String, String>();
	private List<String> ad_list = new ArrayList<String>();
	/* (non-Javadoc)
	 * @see com.adwo.SendAdvNdata.SendDataInterface#GetDataFromMySQL()
	 * 
	 * @flag : switch between show and click
	 * @period_flag : switch between day and hour
	 */
	@Override
	public void GetDataFromMySQL(String propertyfile, String date, int limit, int flag, int period_flag) {
		// TODO Auto-generated method stub
		try {
			String table = null;
			String pick_value = null;
			String pick_field = null;
			if(flag == 1)
			{
				table = "adv_show_n";
				pick_field = "show_count";
				pick_value = "puv";
			}
			else
			{
				table = "adv_click_n";
				pick_field = "click_count";
				pick_value = "cuv";
			}
			Connection conn = MySQLconnection.getConnection(propertyfile);
			String query = "SELECT advid, " + pick_field + ", num FROM " + table + " where createtime = '" + date + "' and " + pick_field + " <= " + limit + " order by advid, " + pick_field + " asc";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next())
			{
				String combinedkey = rs.getString(1) + "-" + rs.getString(2);
				result_map.put(combinedkey, rs.getString(3));
				if(!ad_list.contains(rs.getString(1)))
					ad_list.add(rs.getString(1));
			}
			
			String uv_query = "select advid, " + pick_value + " from adv_data_monitor where createtime = '" + date + "'";
			rs = stmt.executeQuery(uv_query);
			while(rs.next())
			{
				uv_map.put(rs.getString(1), rs.getString(2));
			}
			rs.close();
			stmt.close();
			conn.close();
			//resultdata format: 2398|2013-09-01 00:00:00|1|1|123, 123, 123, 123, 123, 123, 123, 123, 123, 123
			
			
			String pass_date = date + " 00:00:00";
			
			String result_regex = "|";
			
			Iterator<String> list_iter = ad_list.iterator();
			while(list_iter.hasNext())
			{
				StringBuilder resultdata = new StringBuilder();
				String ad_id = list_iter.next();
				resultdata.append(ad_id).append(result_regex).append(pass_date).append(result_regex)
				.append(period_flag).append(result_regex).append(flag).append(result_regex);
				for(int i = 1; i < limit; i++)
				{
					String series = String.valueOf(i);
					String key = ad_id + "-" + series;
					if(result_map.containsKey(key))
					{
						//result can be found
						if(i == 1)
							resultdata.append(result_map.get(key));
						else
							resultdata.append(",").append(result_map.get(key));
					}
					else
					{
						resultdata.append(",");
					}
				}
				resultdata.append(",").append(uv_map.get(ad_id));
				System.out.println(resultdata.toString());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see com.adwo.SendAdvNdata.SendDataInterface#SendDataToMQ(java.lang.String, java.lang.String)
	 */
	@Override
	public void SendDataToMQ(String quene_name, String msg) {
		// TODO Auto-generated method stub
		
	}

}
