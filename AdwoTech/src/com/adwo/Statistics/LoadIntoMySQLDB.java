/**
 * 
 */
package com.adwo.Statistics;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Iterator;

import com.adwo.DBconnection.MySQLconnection;

/**
 * @author dev
 *
 */
public class LoadIntoMySQLDB {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		LoadIntoMySQLDB lim = new LoadIntoMySQLDB();
		lim.LoadData(args[0], args[1], args[2], args[3], args[4]);
		//lim.LoadData("externalsrc/click", "externalsrc/show", "2013-10-14", "1", "externalsrc/db.properties");
	}

	/**
	 * @param click_file
	 * @param show_file
	 * @param date
	 * @param table_name
	 * @param db.properties
	 * @throws FileNotFoundException 
	 * @throws UnsupportedEncodingException 
	 */
	public void LoadData(String click_file, String show_file, String date, String flag, String dbfile) throws Exception
	{
		InputStreamReader ClickFile = new InputStreamReader(new FileInputStream(click_file),"UTF-8");
		BufferedReader ClickFileReader = new BufferedReader(ClickFile);
		
		InputStreamReader ShowFile = new InputStreamReader(new FileInputStream(show_file),"UTF-8");
		BufferedReader ShowFileReader = new BufferedReader(ShowFile);
		
		HashMap<String,String> Adv_Key_Pv = new HashMap<String, String>();
		HashMap<String,String> Adv_Key_Puv = new HashMap<String, String>();
		HashMap<String,String> Adv_Key_Cv = new HashMap<String, String>();
		HashMap<String,String> Adv_Key_Cuv = new HashMap<String, String>();
		HashMap<String,String> Adv_Key_Click = new HashMap<String, String>();

		String temp = null;
		while((temp = ClickFileReader.readLine()) != null)
		{
			System.out.println(temp);
			String[] details = temp.split("\\s+");
			String combineid = details[0];
			String rclick = details[1];
			String click = details[2];
			String cuv = details[3];
			
			Adv_Key_Cv.put(combineid, rclick);
			Adv_Key_Cuv.put(combineid, cuv);
			Adv_Key_Click.put(combineid, click);
		}
		ClickFileReader.close();

		while((temp = ShowFileReader.readLine()) != null)
		{
			String[] details = temp.split("\\s+");
			String combineid = details[0];
			String pv = details[1];
			String puv = details[2];
			
			Adv_Key_Pv.put(combineid, pv);
			Adv_Key_Puv.put(combineid, puv);
		}
		ShowFileReader.close();
		
		Iterator<String> key = Adv_Key_Pv.keySet().iterator();
		int count = 0;
		StringBuffer resultData = new StringBuffer();
		while(key.hasNext())
		{
			String real = key.next();
			if(!Adv_Key_Cv.containsKey(real))
			{
				if(count == 0)
				{
					resultData
					.append(real).append(",")
					.append(date).append(",")
					.append(Adv_Key_Puv.get(real))
					.append(",")
					.append("0,0,0,")
					.append(Adv_Key_Pv.get(real));
				}
				else
				{
					resultData.append("|")
					.append(real).append(",")
					.append(date).append(",")
					.append(Adv_Key_Puv.get(real))
					.append(",")
					.append("0,0,0,")
					.append(Adv_Key_Pv.get(real));
				}
			}
			else
			{
				if(count == 0)
				{
					resultData.append(real).append(",")
					.append(date).append(",")
					.append(Adv_Key_Puv.get(real)).append(",")
					.append(Adv_Key_Cuv.get(real)).append(",")
					.append(Adv_Key_Click.get(real)).append(",")
					.append(Adv_Key_Cv.get(real)).append(",")
					.append(Adv_Key_Pv.get(real));
				}
				else
				{
					resultData.append("|")
					.append(real).append(",")
					.append(date).append(",")
					.append(Adv_Key_Puv.get(real)).append(",")
					.append(Adv_Key_Cuv.get(real)).append(",")
					.append(Adv_Key_Click.get(real)).append(",")
					.append(Adv_Key_Cv.get(real)).append(",")
					.append(Adv_Key_Pv.get(real));
				}
			}
			count++;
		}
		PostDataToPentahoDB(resultData.toString(),Integer.valueOf(flag),dbfile);
	}

	public static void PostDataToPentahoDB(String resultData, int flag, String dbfile)
			throws Exception {
		System.out.println("Posting to 226 pentaho db");
		//System.out.println(resultData);
		//resultData = resultData.replace("resultData=", "");
		System.out.println(resultData);
		String table_name = null;
		switch (flag) {
		case 1:
			table_name = "report_adv_prg_test";
			break;
		case 2:
			table_name = "report_adv_area";
			break;
		case 3:
			table_name = "report_adv_model";
			break;
		case 4:
			table_name = "report_adv_area_appfun";
			break;
		case 5:
			table_name = "report_adv_prg_appfun_android";
			break;
		case 6:
			table_name = "report_prg_area";
			break;
		case 7:
			table_name = "report_prg_model";
			break;
		case 8:
			table_name = "report_adv_hour";
			break;
		case 9:
			table_name = "report_adv_network";
			break;
		case 10:
			table_name = "report_adv_jailbreak";
			break;
		case 11:
			table_name = "report_adv_operator";
			break;
		}
		System.out.println(flag + " : " + table_name);

		Connection conn = MySQLconnection.getConnection(dbfile);

		String[] details = resultData.split("\\|");
		for (int iterator = 0; iterator < details.length; iterator++) {
			String cur_line = details[iterator];
			String[] cur_details = cur_line.split(",");
			String advid = cur_details[0];
			String app_id = cur_details[1];
			String createtime = cur_details[2];
			String puv = cur_details[3];
			String cuv = cur_details[4];
			String click = cur_details[5];
			String rclick = cur_details[6];
			String pv = cur_details[7];

			switch (flag) {
				case 1: {
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,prgid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " prgid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
					System.out.println(iterator);
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.parseInt(app_id));
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 2: {
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,areaid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " areaid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.parseInt(app_id));
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 3: {
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,modelid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " modelid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.parseInt(app_id));
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 4: {
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,areaid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " areaid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.parseInt(app_id));
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 5: {
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,prgid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " prgid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.parseInt(app_id));
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 6: {
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (prgid,areaid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE prgid ='"
									+ advid + "'," + " areaid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.parseInt(app_id));
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 7: {
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (prgid,areaid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE prgid ='"
									+ advid + "'," + " modelid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setInt(2, Integer.parseInt(app_id));
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 8: {
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,hourid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " hourid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setString(2, app_id);
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 9:
				{
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,networkid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " networkid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setString(2, app_id);
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 10:
				{
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,jailbreakid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " jailbreakid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setString(2, app_id);
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
				case 11:
				{
					PreparedStatement preStmt = conn
							.prepareStatement("INSERT INTO "
									+ table_name
									+ " (advid,operatorid,createtime,puv,cuv,click,rclick,pv)"
									+ "values(?,?,?,?,?,?,?,?)  ON DUPLICATE KEY UPDATE advid ='"
									+ advid + "'," + " jailbreakid = '" + app_id + "',"
									+ " createtime = '" + createtime + "',"
									+ " puv = '" + puv + "'," + " cuv = '" + cuv
									+ "'," + " click = '" + click + "',"
									+ " rclick = '" + rclick + "'," + " pv = '"
									+ pv + "'");
					conn.setAutoCommit(false);
	
					preStmt.setInt(1, Integer.parseInt(advid));
					preStmt.setString(2, app_id);
					preStmt.setString(3, createtime);
					preStmt.setInt(4, Integer.parseInt(puv));
					preStmt.setInt(5, Integer.parseInt(cuv));
					preStmt.setInt(6, Integer.parseInt(click));
					preStmt.setInt(7, Integer.parseInt(rclick));
					preStmt.setInt(8, Integer.parseInt(pv));
	
					int j = preStmt.executeUpdate();
					conn.commit();
					if (j == 0)
						continue;
					preStmt.close();
					break;
				}
			}

		}

		conn.close();
		System.out.println("Posting to 226 pentaho db is finished!");
	}
}
