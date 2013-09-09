/**
 * 
 */
package com.adwo.AdvDataMonitor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Set;

import com.adwo.DBconnection.HiveConnection;
import com.adwo.DBconnection.MySQLconnection;

/**
 * @author dev
 *
 */
public class Monitor {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Monitor	m = new Monitor();
		m.AdvDataMonitor("2013-08-28", 1, "externalsrc/db.properties");

		/*String str = null;
		for (int date = 21; date >= 12; date--)
		{
			if(date >= 10)
				str = "2013-08-" + date;
			else
				str = "2013-08-0" + date;
			m.AdvDataMonitor(str, 2, "externalsrc/db.properties");
		}*/
		//m.AdvDataMonitor(args[0], Integer.parseInt(args[1]), args[2]);
	}

	/**
	 * 
	 * @param date
	 * @param flag	0 == Platform || 1 == Separate adv || 2 == device data monitor
	 */
	public void AdvDataMonitor(String date, int flag, String propertyfile)
	{
		try{
			HashMap<String, String> Platform_data_map = new HashMap<String, String>();
			
			Connection hive_conn = HiveConnection.getConnection();
			if(flag == 0)
			{
				int plat_pv = 0;
				int plat_puv = 0;
				int plat_click = 0; 
				int plat_cuv = 0;
				
				Statement platform_stmt = hive_conn.createStatement();
				String platform_pv_query = "select count(udid),count(distinct(udid)) from ios_show_log where dt = '" + date + "'";
				ResultSet platform_pv_rs = platform_stmt.executeQuery(platform_pv_query);
				while(platform_pv_rs.next())
				{
					plat_pv = platform_pv_rs.getInt(1);
					plat_puv = platform_pv_rs.getInt(2);
				}
				platform_pv_rs.close();
				
				String platform_cv_query = "select count(udid),count(distinct(udid)) from ios_click_log where dt = '" + date + "'";
				ResultSet platform_cv_rs = platform_stmt.executeQuery(platform_cv_query);
				while(platform_cv_rs.next())
				{
					plat_click = platform_cv_rs.getInt(1);
					plat_cuv = platform_cv_rs.getInt(2);
				}
				platform_cv_rs.close();
				Platform_data_map.put("ios", plat_pv + "," + plat_puv + "," + plat_click + "," + plat_cuv);
				//Android
				platform_pv_query = "select count(udid),count(distinct(udid)) from show_log where dt = '" + date + "'";
				ResultSet android_platform_pv_rs = platform_stmt.executeQuery(platform_pv_query);
				while(android_platform_pv_rs.next())
				{
					plat_pv = android_platform_pv_rs.getInt(1);
					plat_puv = android_platform_pv_rs.getInt(2);
				}
				android_platform_pv_rs.close();
				platform_cv_query = "select count(udid),count(distinct(udid)) from click_log where dt = '" + date + "'";
				ResultSet android_platform_cv_rs = platform_stmt.executeQuery(platform_cv_query);
				while(android_platform_cv_rs.next())
				{
					plat_click = android_platform_cv_rs.getInt(1);
					plat_cuv = android_platform_cv_rs.getInt(2);
				}
				android_platform_cv_rs.close();
				Platform_data_map.put("android", plat_pv + "," + plat_puv + "," + plat_click + "," + plat_cuv);
				//Appfun
				platform_pv_query = "select count(udid),count(distinct(udid)) from ios_fsshow_log where dt = '" + date + "'";
				ResultSet appfun_platform_pv_rs = platform_stmt.executeQuery(platform_pv_query);
				while(appfun_platform_pv_rs.next())
				{
					plat_pv = appfun_platform_pv_rs.getInt(1);
					plat_puv = appfun_platform_pv_rs.getInt(2);
				}
				appfun_platform_pv_rs.close();
				platform_cv_query = "select count(udid),count(distinct(udid)) from ios_fsclick_log where dt = '" + date + "'";
				ResultSet appfun_platform_cv_rs = platform_stmt.executeQuery(platform_cv_query);
				while(appfun_platform_cv_rs.next())
				{
					plat_click = appfun_platform_cv_rs.getInt(1);
					plat_cuv = appfun_platform_cv_rs.getInt(2);
				}
				appfun_platform_cv_rs.close();
				Platform_data_map.put("appfun", plat_pv + "," + plat_puv + "," + plat_click + "," + plat_cuv);
				
				Set<String> platform_key = Platform_data_map.keySet();
				int platformid = 0;
				for(String pk : platform_key)
				{
					//System.out.println(pk + "===" + Platform_data_map.get(pk));
					String[] data = Platform_data_map.get(pk).split(",");
					int pv = Integer.parseInt(data[0]);
					int puv = Integer.parseInt(data[1]);
					int cv = Integer.parseInt(data[2]);
					int cuv = Integer.parseInt(data[3]);
					
					if(pk == "ios")			
						platformid = 3;
					else if(pk == "android")	
						platformid = 2;
					else 				
						platformid = 1;
					Connection mysql_conn = MySQLconnection.getConnection(propertyfile);
					PreparedStatement preStmt=mysql_conn.prepareStatement("REPLACE INTO platform_data_monitor "+ 
							" (platformid,createtime,pv,puv,cv,cuv)"+
							"values(?,?,?,?,?,?)");
					mysql_conn.setAutoCommit(false);
					preStmt.setInt(1, platformid);
					preStmt.setString(2, date);
					preStmt.setInt(3, pv);
					preStmt.setInt(4, puv);
					preStmt.setInt(5, cv);
					preStmt.setInt(6, cuv);
					
					int j = preStmt.executeUpdate();
					mysql_conn.commit();
					if(j!=0)
						//System.out.println("Insert/OR/Update local db done!");
					preStmt.close();
				}
			}
			else if(flag == 1)
			{
				HashMap<Integer, Integer> Adv_pv_map = new HashMap<Integer, Integer>();
				HashMap<Integer, Integer> Adv_puv_map = new HashMap<Integer, Integer>();
				HashMap<Integer, Integer> Adv_cv_map = new HashMap<Integer, Integer>();
				HashMap<Integer, Integer> Adv_cuv_map = new HashMap<Integer, Integer>();
				
				String adv_pv_query = "select ad_id, count(udid), count(distinct(udid)) from ios_show_log where dt = '" + date + "' group by ad_id";
				Statement adv_stmt = hive_conn.createStatement();
				ResultSet adv_pv_rs = adv_stmt.executeQuery(adv_pv_query);
				while(adv_pv_rs.next())
				{
					int ad_id = adv_pv_rs.getInt(1);
					int adv_pv = adv_pv_rs.getInt(2);
					Adv_pv_map.put(ad_id, adv_pv);
					int adv_puv = adv_pv_rs.getInt(3);
					Adv_puv_map.put(ad_id, adv_puv);
				}
				adv_pv_rs.close();
								
				String adv_cv_query = "select ad_id, count(udid), count(distinct(udid)) from ios_click_log where dt = '" + date + "' group by ad_id";
				ResultSet adv_cv_rs = adv_stmt.executeQuery(adv_cv_query);
				while(adv_cv_rs.next())
				{
					int ad_id = adv_cv_rs.getInt(1);
					int adv_cv = adv_cv_rs.getInt(2);
					Adv_cv_map.put(ad_id, adv_cv);
					int adv_cuv = adv_cv_rs.getInt(3);
					Adv_cuv_map.put(ad_id, adv_cuv);
				}
				adv_cv_rs.close();
								
				//android
				String android_adv_pv_query = "select ad_id, count(udid), count(distinct(udid)) from show_log where dt = '" + date + "' group by ad_id";
				ResultSet android_adv_pv_rs = adv_stmt.executeQuery(android_adv_pv_query);
				while(android_adv_pv_rs.next())
				{
					int ad_id = android_adv_pv_rs.getInt(1);
					int adv_pv = android_adv_pv_rs.getInt(2);
					Adv_pv_map.put(ad_id, adv_pv);
					int adv_puv = android_adv_pv_rs.getInt(3);
					Adv_puv_map.put(ad_id, adv_puv);
				}
				android_adv_pv_rs.close();
								
				String android_adv_cv_query = "select ad_id, count(udid), count(distinct(udid)) from click_log where dt = '" + date + "' group by ad_id";
				ResultSet android_adv_cv_rs = adv_stmt.executeQuery(android_adv_cv_query);
				while(android_adv_cv_rs.next())
				{
					int ad_id = android_adv_cv_rs.getInt(1);
					int adv_cv = android_adv_cv_rs.getInt(2);
					Adv_cv_map.put(ad_id, adv_cv);
					int adv_cuv = android_adv_cv_rs.getInt(3);
					Adv_cuv_map.put(ad_id, adv_cuv);
				}
				android_adv_cv_rs.close();
								
				//appfun
				String appfun_adv_pv_query = "select ad_id, count(udid), count(distinct(udid)) from ios_fsshow_log where dt = '" + date + "' group by ad_id";
				ResultSet appfun_adv_pv_rs = adv_stmt.executeQuery(appfun_adv_pv_query);
				while(appfun_adv_pv_rs.next())
				{
					int ad_id = appfun_adv_pv_rs.getInt(1);
					int adv_pv = appfun_adv_pv_rs.getInt(2);
					Adv_pv_map.put(ad_id, adv_pv);
					int adv_puv = appfun_adv_pv_rs.getInt(3);
					Adv_puv_map.put(ad_id, adv_puv);
				}
				appfun_adv_pv_rs.close();
								
				String appfun_adv_cv_query = "select ad_id, count(udid), count(distinct(udid)) from ios_fsclick_log where dt = '" + date + "' group by ad_id";
				ResultSet appfun_adv_cv_rs = adv_stmt.executeQuery(appfun_adv_cv_query);
				while(appfun_adv_cv_rs.next())
				{
					int ad_id = appfun_adv_cv_rs.getInt(1);
					int adv_cv = appfun_adv_cv_rs.getInt(2);
					Adv_cv_map.put(ad_id, adv_cv);
					int adv_cuv = appfun_adv_cv_rs.getInt(3);
					Adv_cuv_map.put(ad_id, adv_cuv);
				}
				appfun_adv_cv_rs.close();
						
				Set<Integer> adv_key = Adv_pv_map.keySet();
				Connection mysql_conn = MySQLconnection.getConnection(propertyfile);
				for(int ak : adv_key)
				{
					if(Adv_cuv_map.get(ak) != null)
					{
						//System.out.println(ak + "=" + Adv_pv_map.get(ak) + "||" + Adv_cuv_map.get(ak));
						int pv = Adv_pv_map.get(ak);
						int puv = Adv_puv_map.get(ak);
						int cv = Adv_cv_map.get(ak);
						int cuv = Adv_cuv_map.get(ak);
						PreparedStatement preStmt=mysql_conn.prepareStatement("REPLACE INTO adv_data_monitor "+ 
								" (advid,createtime,pv,puv,cv,cuv)"+
								"values(?,?,?,?,?,?)");
						mysql_conn.setAutoCommit(false);
						preStmt.setInt(1, ak);
						preStmt.setString(2, date);
						preStmt.setInt(3, pv);
						preStmt.setInt(4, puv);
						preStmt.setInt(5, cv);
						preStmt.setInt(6, cuv);
						
						int j = preStmt.executeUpdate();
						mysql_conn.commit();
						if(j!=0)
							//System.out.println("Insert/OR/Update local db done!");
						preStmt.close();
					}
				}
				mysql_conn.close();
			}
			else if(flag == 2)
			{
				HashMap<Integer, Integer> Device_pv_map = new HashMap<Integer, Integer>();
				HashMap<Integer, Integer> Device_puv_map = new HashMap<Integer, Integer>();
				HashMap<Integer, Integer> Device_cv_map = new HashMap<Integer, Integer>();
				HashMap<Integer, Integer> Device_cuv_map = new HashMap<Integer, Integer>();
				
				String adv_pv_query = "select device_id, count(udid), count(distinct(udid)) from ios_show_log where dt = '" + date + "' group by device_id";
				Statement adv_stmt = hive_conn.createStatement();
				ResultSet adv_pv_rs = adv_stmt.executeQuery(adv_pv_query);
				while(adv_pv_rs.next())
				{
					int ad_id = adv_pv_rs.getInt(1);
					int adv_pv = adv_pv_rs.getInt(2);
					Device_pv_map.put(ad_id, adv_pv);
					int adv_puv = adv_pv_rs.getInt(3);
					Device_puv_map.put(ad_id, adv_puv);
				}
				adv_pv_rs.close();
								
				String adv_cv_query = "select device_id, count(udid), count(distinct(udid)) from ios_click_log where dt = '" + date + "' group by device_id";
				ResultSet adv_cv_rs = adv_stmt.executeQuery(adv_cv_query);
				while(adv_cv_rs.next())
				{
					int ad_id = adv_cv_rs.getInt(1);
					int adv_cv = adv_cv_rs.getInt(2);
					Device_cv_map.put(ad_id, adv_cv);
					int adv_cuv = adv_cv_rs.getInt(3);
					Device_cuv_map.put(ad_id, adv_cuv);
				}
				adv_cv_rs.close();
								
				//android
				String android_adv_pv_query = "select device_id, count(udid), count(distinct(udid)) from show_log where dt = '" + date + "' and device_id is not NULL group by device_id";
				ResultSet android_adv_pv_rs = adv_stmt.executeQuery(android_adv_pv_query);
				while(android_adv_pv_rs.next())
				{
					int ad_id = 0;
					if(!String.valueOf(android_adv_pv_rs.getInt(1)).contains("_")&&android_adv_pv_rs.getString(1).length() > 1)
					{
						ad_id = android_adv_pv_rs.getInt(1);
						int adv_pv = android_adv_pv_rs.getInt(2);
						Device_pv_map.put(ad_id, adv_pv);
						int adv_puv = android_adv_pv_rs.getInt(3);
						Device_puv_map.put(ad_id, adv_puv);
					}
					else
						continue;
				}
				android_adv_pv_rs.close();
								
				String android_adv_cv_query = "select device_id, count(udid), count(distinct(udid)) from click_log where dt = '" + date + "' and device_id is not NULL group by device_id";
				ResultSet android_adv_cv_rs = adv_stmt.executeQuery(android_adv_cv_query);
				while(android_adv_cv_rs.next())
				{
					int ad_id = 0;
					if(!android_adv_cv_rs.getString(1).contains("_") && android_adv_cv_rs.getString(1).length() > 1)
					{	
						ad_id = android_adv_cv_rs.getInt(1);
						int adv_cv = android_adv_cv_rs.getInt(2);
						Device_cv_map.put(ad_id, adv_cv);
						int adv_cuv = android_adv_cv_rs.getInt(3);
						Device_cuv_map.put(ad_id, adv_cuv);
					}
					else continue;
				}
				android_adv_cv_rs.close();
								
				//appfun
				String appfun_adv_pv_query = "select device_id, count(udid), count(distinct(udid)) from ios_fsshow_log where dt = '" + date + "' group by device_id";
				ResultSet appfun_adv_pv_rs = adv_stmt.executeQuery(appfun_adv_pv_query);
				while(appfun_adv_pv_rs.next())
				{
					int ad_id = appfun_adv_pv_rs.getInt(1);
					int adv_pv = appfun_adv_pv_rs.getInt(2);
					Device_pv_map.put(ad_id, adv_pv);
					int adv_puv = appfun_adv_pv_rs.getInt(3);
					Device_puv_map.put(ad_id, adv_puv);
				}
				appfun_adv_pv_rs.close();
								
				String appfun_adv_cv_query = "select device_id, count(udid), count(distinct(udid)) from ios_fsclick_log where dt = '" + date + "' group by device_id";
				ResultSet appfun_adv_cv_rs = adv_stmt.executeQuery(appfun_adv_cv_query);
				while(appfun_adv_cv_rs.next())
				{
					int ad_id = appfun_adv_cv_rs.getInt(1);
					int adv_cv = appfun_adv_cv_rs.getInt(2);
					Device_cv_map.put(ad_id, adv_cv);
					int adv_cuv = appfun_adv_cv_rs.getInt(3);
					Device_cuv_map.put(ad_id, adv_cuv);
				}
				appfun_adv_cv_rs.close();
						
				Set<Integer> adv_key = Device_pv_map.keySet();
				Connection mysql_conn = MySQLconnection.getConnection(propertyfile);
				for(int ak : adv_key)
				{
					if(Device_cuv_map.get(ak) != null && !String.valueOf(ak).contains("_"))
					{
						//System.out.println(ak + "=" + Adv_pv_map.get(ak) + "||" + Adv_cuv_map.get(ak));
						int pv = Device_pv_map.get(ak);
						int puv = Device_puv_map.get(ak);
						int cv = Device_cv_map.get(ak);
						int cuv = Device_cuv_map.get(ak);
						PreparedStatement preStmt=mysql_conn.prepareStatement("INSERT INTO device_data_monitor "+ 
								" (modelid,createtime,pv,puv,cv,cuv)"+
								"values(?,?,?,?,?,?)");
						mysql_conn.setAutoCommit(false);
						preStmt.setInt(1, ak);
						preStmt.setString(2, date);
						preStmt.setInt(3, pv);
						preStmt.setInt(4, puv);
						preStmt.setInt(5, cv);
						preStmt.setInt(6, cuv);
						
						int j = preStmt.executeUpdate();
						mysql_conn.commit();
						if(j!=0)
							//System.out.println("Insert/OR/Update local db done!");
						preStmt.close();
					}
				}
				mysql_conn.close();
			}
			System.out.println("done.");
			hive_conn.close();
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
