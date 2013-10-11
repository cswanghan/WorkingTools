/**
 * 
 */
package com.adwo.SendAdvNdata;

/**
 * @author dev
 *
 */
public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SendAdvNdataToMQ samq = new SendAdvNdataToMQ();
		samq.GetDataFromMySQL("externalsrc/db.properties", "2013-09-25", 10, 1, 1);
	}

}
