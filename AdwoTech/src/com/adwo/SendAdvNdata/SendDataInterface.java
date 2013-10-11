/**
 * 
 */
package com.adwo.SendAdvNdata;

/**
 * @author dev
 *
 */
public interface SendDataInterface {
	
	void GetDataFromMySQL(String db_property, String date, int show_limit, int flag, int period_flag);
	void SendDataToMQ(String quene_name, String msg);
}
