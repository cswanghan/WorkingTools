/**
 * 
 */
package com.adwo.GetAdvNdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.adwo.DBconnection.MySQLconnection;

/**
 * @author dev
 *
 */
public class CumulativeCaculator {

	/**
	 * 
	 * @param inputfile	currently using file as input, next step will switch to DB
	 * @param outputfile	
	 * @param regex		","
	 * @param threshold	Theshold for the distance
	 * @param flag		0 == banner || 1 == appfun
	 * @throws Exception
	 */
	public void SplitReader(String regex, double threshold, int flag, String propertyfile, String date) throws Exception
	{
		Connection mysql_conn = MySQLconnection.getConnection(propertyfile);
		int rows = 0;
		ArrayList[] array = null;
		if(flag == 0)
		{
			//String banner_query = "SELECT c.advname, a.show_n, a.count, b.num, a.advid FROM `adv_n_plus_data` a " +
			//		"join adv_show_n b on a.advid = b.advid and a.createtime = b.createtime and a.show_n = b.show_count join adv_dimension_test c on a.advid = c.id " +
			//		"where a.createtime = '2013-07-17' group by c.advname, a.show_n order by c.advname asc;";
			
			String banner_query = "select adv_name, show_n, count, num, advid from banner_adv_n_plus_result where createtime = '" + date + "'";
			String rs_result = "SELECT count(*) FROM banner_adv_n_plus_result a join adv_dimension_test c on a.advid = c.id where a.createtime = '" + date +"' and c.adv_display_id <> 4";
			Statement stmt = mysql_conn.createStatement();
			ResultSet rs_count = stmt.executeQuery(rs_result);
			//int rows = 0;
			while(rs_count.next())
			{
				rows = rs_count.getInt(1);
			}
			array = new ArrayList[rows];
		        int l = 0;
		        ResultSet result_rs = stmt.executeQuery(banner_query);
		        while (result_rs.next()) {
		                array[l] = new ArrayList();
		                array[l].add(0,result_rs.getString(1));	//advname
		                array[l].add(1,result_rs.getString(2));	//show_n		                
		                array[l].add(2,result_rs.getString(3));	//count
		                array[l].add(3,result_rs.getString(4));	//num
		                array[l].add(4,result_rs.getString(5));	//advid
		                l++;
		        }
		}
		else
		{
			//String appfun_query = "SELECT c.AdvName, a.show_n, a.count, b.num,a.advid FROM `adv_n_plus_data` a join adv_show_n b on a.advid = b.advid " +
			//		"join adv_dimension_test c on a.advid = c.id and a.createtime = b.createtime and a.show_n = b.show_count " +
			//		"where a.createtime = '2013-07-17' and c.adv_display_id = 4 group by c.AdvName, a.show_n order by c.advname ";
			
			String appfun_query = "select adv_name, show_n, count, num, advid from appfun_adv_n_plus_result where createtime = '" + date + "'";
			String rs_result = "SELECT count(*) FROM appfun_adv_n_plus_result a join adv_dimension_test c on a.advid = c.id where a.createtime = '" + date + "' and c.adv_display_id = 4";
			Statement stmt = mysql_conn.createStatement();
			ResultSet rs_count = stmt.executeQuery(rs_result);
			//int rows = 0;
			while(rs_count.next())
			{
				rows = rs_count.getInt(1);
			}
			array = new ArrayList[rows];
		        int l = 0;
		        ResultSet result_rs = stmt.executeQuery(appfun_query);
		        while (result_rs.next()) {
		                array[l] = new ArrayList();
		                array[l].add(0,result_rs.getString(1));	//advname
		                array[l].add(1,result_rs.getString(2));	//show_n		                
		                array[l].add(2,result_rs.getString(3));	//count
		                array[l].add(3,result_rs.getString(4));	//num
		                array[l].add(4,result_rs.getString(5));	//advid
		                System.out.println(result_rs.getString(1));
		                l++;
		        }
		}
		
		String temp_groupname = null;
		String combined_groupname = null;
		HashMap<String, String> tempMap = new HashMap<String, String>();
		for(int iter = 0; iter < rows - 1; iter++)
		{
			//System.out.println(array[iter].get(0) + ":" + array[iter].get(1));
			//System.out.println(iter);
			String groupname = String.valueOf(array[iter].get(0));
			String ad_id = String.valueOf(array[iter].get(4));
			combined_groupname = ad_id + "," + groupname;
			if(temp_groupname == null)
			{
				temp_groupname = groupname;
				tempMap.put(String.valueOf(array[iter].get(1)), String.valueOf(array[iter].get(2)) + regex + String.valueOf(array[iter].get(3)));
			}
			else if(temp_groupname == groupname || temp_groupname.equals(groupname))
			{
				tempMap.put(String.valueOf(array[iter].get(1)), String.valueOf(array[iter].get(2)) + regex + String.valueOf(array[iter].get(3)));
			}
			else if(temp_groupname != null && !temp_groupname.equals(groupname))
			{
				SplitWriter(tempMap, combined_groupname, regex, threshold, propertyfile, date);
				tempMap.clear();
				temp_groupname = groupname;
				tempMap.put(String.valueOf(array[iter].get(1)), String.valueOf(array[iter].get(2)) + regex + String.valueOf(array[iter].get(3)));
			}
		}
		/*InputStreamReader InputFile = new InputStreamReader(new FileInputStream(inputfile),"gbk");
		BufferedReader FileReader = new BufferedReader(InputFile);
		
		String line = null;
		String temp_groupname = null;
		String combined_groupname = null;
		HashMap<String, String> tempMap = new HashMap<String, String>();
		 * while( (line = FileReader.readLine()) != null )
		{
			String[] details = line.split(regex);
			String groupname = details[0];
			String ad_id = details[4];
			combined_groupname = ad_id + "," + groupname;
			if(temp_groupname == null)
			{
				temp_groupname = groupname;
				tempMap.put(details[1], details[2] + regex + details[3]);
			}
			else if(temp_groupname == groupname || temp_groupname.equals(groupname))
			{
				tempMap.put(details[1], details[2] + regex + details[3]);
			}
			else if(temp_groupname != null && !temp_groupname.equals(groupname))
			{
				SplitWriter(tempMap, outputfile, combined_groupname, regex, threshold);
				tempMap.clear();
				temp_groupname = groupname;
				tempMap.put(details[1], details[2] + regex + details[3]);
			}
		}*/
		SplitWriter(tempMap, combined_groupname, regex, threshold, propertyfile,date);
	}
	
	/**
	 * 
	 * @param AdvInfoMap
	 * @param outputfilepath
	 * @param groupname
	 * @param regex
	 * @param threshold
	 * @throws Exception
	 */
	public void SplitWriter(HashMap<String, String> AdvInfoMap, String groupname, String regex, double threshold, String propertyfile, String date) throws Exception
	{
		//String OutPutFilePath = outputfilepath + "cumulative";
		//BufferedWriter FileWriter = new BufferedWriter(new FileWriter(OutPutFilePath,true));
		
		//Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfilepath,true)));
		//OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(outputfilepath,true),"gbk");
		//BufferedWriter writer = new BufferedWriter(write);  
		
		
		HashMap<Integer,String> resultMap = Caculator(AdvInfoMap,regex,groupname,threshold,propertyfile,date);
		
		/*Set<Integer> keyset = resultMap.keySet();
		for(Integer s : keyset)
		{
			writer.write(groupname + "," + s + "," + resultMap.get(s) + "\n");
		}
		writer.close();*/
	}
	
	/**
	 * 
	 * @param BestNMap
	 * @param outputfilepath
	 * @param groupname
	 * @param regex
	 * @param threshold
	 * @throws Exception
	 */
	public void BestNWriter(HashMap<Integer, String> BestNMap, String outputfilepath, String groupname, String regex, double threshold) throws Exception
	{
		OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(outputfilepath,true),"gbk");
		BufferedWriter writer = new BufferedWriter(write);  
				
		Set<Integer> keyset = BestNMap.keySet();
		for(Integer s : keyset)
		{
			String[] details = BestNMap.get(s).split(regex);
			String clickcumulative = details[0];
			String showcumulative = details[1];
			String ratio = details[2];
			if(Double.parseDouble(ratio) > threshold)
				writer.write(groupname + ",推荐N值," + s + "," + "点击累积比=" + clickcumulative + ",展示累积比=" + showcumulative + ",节省比=" + ratio + "\n");
		}
		writer.close();
	}
	
	/**
	 * 
	 * @param BestNMap
	 * @param outputfilepath
	 * @param groupname
	 * @param regex
	 * @throws Exception
	 */
	public void OptimalNWriter(HashMap<String, String> BestNMap, String outputfilepath, String groupname, String regex, String propertyfile, String date) throws Exception
	{
		//groupname = 6157,iwatch(weico)-iPhone-banner-20130705-1
		System.out.println(groupname);
		String[] name_details = groupname.split(",");
		String advid = name_details[0];
		Connection mysql_conn = MySQLconnection.getConnection(propertyfile);
		int count = 0; 
		Iterator<String> keyset = BestNMap.keySet().iterator();
		while(keyset.hasNext())
		{
				String s = keyset.next();
				String[] details = BestNMap.get(s).split(regex);
				String optimaln = details[0];
				String clickcumulative = details[1];
				String showcumulative = details[2];
				PreparedStatement preStmt = mysql_conn.prepareStatement("INSERT INTO adv_optimal_n_data "+ 
						" (advid,history_rec_n,createtime,click_cumulative,show_cumulative)"+
						"values(?,?,?,?,?)");
				mysql_conn.setAutoCommit(false);
				//System.out.println(key.toString());
				preStmt.setInt(1, Integer.parseInt(advid));
				preStmt.setInt(2, Integer.valueOf(optimaln));
				preStmt.setString(3, date);
				preStmt.setFloat(4, Float.parseFloat(clickcumulative));
				preStmt.setFloat(5, Float.parseFloat(showcumulative));
				int j = preStmt.executeUpdate();
				mysql_conn.commit();
				if(j!=0)
					//System.out.println("Insert/OR/Update local db done!");
				preStmt.close();
		}
		
		/*OutputStreamWriter write = new OutputStreamWriter(new FileOutputStream(outputfilepath,true),"gbk");
		BufferedWriter writer = new BufferedWriter(write);  
				
		Set<String> keyset = BestNMap.keySet();
		for(String s : keyset)
		{
			String[] details = BestNMap.get(s).split(regex);
			String optimaln = details[0];
			String clickcumulative = details[1];
			String showcumulative = details[2];
			writer.write(groupname + ",局部最优N值," + optimaln + "," + "点击累积比=" + clickcumulative + ",展示累积比=" + showcumulative + "\n");
		}
		writer.close();*/
	}
	
	/**
	 * This function will do cumulative calculation and return the result 
	 * in a hashMap. 
	 */
	public HashMap<Integer, String> Caculator(HashMap<String, String> PrevCalcMap, String regex, String groupname, double threshold, String propertyfile, String date)
	{
		HashMap<Integer, String> resultMap = new HashMap<Integer, String>();
		
		HashMap<Integer, Double> EfficiencyMap = new HashMap<Integer, Double>();
		HashMap<Integer, Double> CumulativeClick = new HashMap<Integer, Double>();
		HashMap<Integer, Double> CumulativeShow = new HashMap<Integer, Double>();
		
		HashMap<Integer, String> BestN = new HashMap<Integer, String>();
		HashMap<String, String> OptimalN = new HashMap<String,String>();
		
		double clickall = 0;
		double showall = 0;
		List<Integer> CountList = new ArrayList<Integer>();
		Set<String> map_keyset = PrevCalcMap.keySet();
		for(String s : map_keyset)		//iteratively visit each cout
		{
			int count = Integer.valueOf(s);
			String[] details = PrevCalcMap.get(s).split(regex);
			double clickcoverage = Double.valueOf(details[0]);
			double showcoverage = Double.valueOf(details[1]);
			Double efficiency = (double) (clickcoverage / (count * showcoverage));
			EfficiencyMap.put(count, efficiency);
			clickall = clickall + clickcoverage;
			showall = showall + (showcoverage * count);
			CountList.add(count);
		}
		Collections.sort(CountList);
		
		List<Double> ClickCumulativeList = new ArrayList<Double>();
		Iterator<Integer> list_counter = CountList.iterator();
		//for(String s : map_keyset)		//iteratively visit each cout
		while(list_counter.hasNext())
		{
			int s = list_counter.next();
			int count = Integer.valueOf(s);
			String[] details = PrevCalcMap.get(String.valueOf(count)).split(regex);
			double clickcoverage = Integer.valueOf(details[0]);
			double current_click_ratio = clickcoverage / clickall;
			ClickCumulativeList.add(current_click_ratio);
			double current_click_cumulative = ListSummer(ClickCumulativeList);
			CumulativeClick.put(count, current_click_cumulative);
		}
		
		List<Double> ShowCumulativeList = new ArrayList<Double>();
		list_counter = CountList.iterator();
		//for(String s : map_keyset)		//iteratively visit each cout
		while(list_counter.hasNext())
		{
			int s = list_counter.next();
			int count = Integer.valueOf(s);
			String[] details = PrevCalcMap.get(String.valueOf(count)).split(regex);
			int showcoverage = Integer.valueOf(details[1]);
			double current_show_ratio = (showcoverage * count) / showall;
			ShowCumulativeList.add(current_show_ratio);
			double current_show_cumulative = ListSummer(ShowCumulativeList);
			CumulativeShow.put(count, current_show_cumulative);
		}
		
		DecimalFormat dcmFmt = new DecimalFormat("0.000000");
		Iterator<Integer> List_iter = CountList.iterator();
		int count = 0;
		double rec_value = 0.0;
		double prev_value = 0.0;
		double distance = 0.0;
		while(List_iter.hasNext())
		{
			int key = List_iter.next();
			resultMap.put(key, dcmFmt.format(EfficiencyMap.get(key)) + "," + dcmFmt.format(CumulativeClick.get(key)) + "," + dcmFmt.format(CumulativeShow.get(key)));
			rec_value = (1 - CumulativeShow.get(key)) / (1 - CumulativeClick.get(key));
			if(rec_value >= threshold && CumulativeClick.get(key) >= 0.7 && CumulativeShow.get(key) <= 0.8)
			{
				if(rec_value >= prev_value && (rec_value - prev_value) >= distance )
				{
					OptimalN.put("Optimal N",key + "," + dcmFmt.format(CumulativeClick.get(key)) + "," + dcmFmt.format(CumulativeShow.get(key)));
					distance = rec_value - prev_value;
				}
				prev_value = rec_value;
			}//This part will get the Optimal N solution with the condition click >= 0.7 and show <= 0.8 and distance >= 1.3
			
			if(CumulativeClick.get(key) >= 0.7 && count == 0)
			{
				if(CumulativeShow.get(key) <= 0.8)
				{
					rec_value = (1 - CumulativeShow.get(key)) / (1 - CumulativeClick.get(key));
					
					BestN.put(key, dcmFmt.format(CumulativeClick.get(key)) + "," + dcmFmt.format(CumulativeShow.get(key)) + "," + rec_value);
					count++;
				}
			}//get click 70% point
			else if(CumulativeClick.get(key) >= 0.8 && count == 1)
			{
				if(CumulativeShow.get(key) <= 0.8)
				{
					rec_value = (1 - CumulativeShow.get(key)) / (1 - CumulativeClick.get(key));
					
					BestN.put(key, dcmFmt.format(CumulativeClick.get(key)) + "," + dcmFmt.format(CumulativeShow.get(key)) + "," + rec_value);
					count++;
				}
			}//get click 80% point
			else if(CumulativeClick.get(key) >= 0.9 && count == 2)
			{
				if(CumulativeShow.get(key) <= 0.8)
				{
					rec_value = (1 - CumulativeShow.get(key)) / (1 - CumulativeClick.get(key));
					
					BestN.put(key, dcmFmt.format(CumulativeClick.get(key)) + "," + dcmFmt.format(CumulativeShow.get(key)) + "," + rec_value);
					count++;
				}
			}//get click 90% point
		}
		
		/*Set<String> set = OptimalN.keySet();
		for (String s : set)
		{
			System.out.println(groupname + "," + s + "," + OptimalN.get(s));
		}*/
		
		try{
			//BestNWriter(BestN, "F:\\BD&AE&Tech需求\\wangwei\\广告N+\\RecNdata\\0704bannerRecNnewnew", groupname, ",", threshold);
			OptimalNWriter(OptimalN, "F:\\BD&AE&Tech需求\\wangwei\\广告N+\\RecNdata\\0704bannerOptimalNnewnew", groupname, ",", propertyfile, date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return resultMap;
	}
	
	/**
	 * 
	 * @param InputList
	 * @return
	 */
	public double ListSummer(List<Double> InputList)
	{
		double result = 0.0;
		
		Iterator<Double> list_iter = InputList.iterator();
		while(list_iter.hasNext())
		{
			Double key = list_iter.next();
			result = result + key;
		}
		return result;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		CumulativeCaculator cc = new CumulativeCaculator();
		//cc.SplitReader("F:\\BD&AE&Tech需求\\wangwei\\广告N+\\N+数据0704.txt", "F:\\BD&AE&Tech需求\\wangwei\\广告N+\\N+数据0704cumulative", "\t", 1.3, 0,"externalsrc/db.properties","2013-07-13");
		cc.SplitReader("\t", 1.3, 0,"externalsrc/db.properties","2013-08-16");
//		if(args.length < 4)
//			System.out.println("Correct Args Format: java -jar CumulativeCalculator.jar <regex> <threshold> <platformid> <path_of_db.propertyfile> <date>");
//		else if(args.length > 5)
//			System.out.println("Correct Args Format: java -jar CumulativeCalculator.jar <regex> <threshold> <platformid> <path_of_db.propertyfile> <date>");
//		else
//			cc.SplitReader(args[0], Float.parseFloat(args[1]), Integer.parseInt(args[2]), args[3], args[4]);
	}

}
