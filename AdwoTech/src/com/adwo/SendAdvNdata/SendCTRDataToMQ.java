/**
 * 
 */
package com.adwo.SendAdvNdata;

import java.sql.ResultSet;
import java.sql.Statement;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.adwo.DBconnection.MySQLconnection;

/**
 * @author dev
 *
 */
public class SendCTRDataToMQ {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		SendCTRDataToMQ s = new SendCTRDataToMQ();
		if(args.length < 3 || args.length > 3)
		{
			System.out.println("Wrong format. Correct Format: java -jar XXX.jar <begindate> <enddate> <path_to_db.properties>");
		}
		else
			s.GetData(args[0], args[1], args[2]);
	}

	public void GetData(String begindate, String enddate, String dbfile) throws Exception
	{
		try{
			java.sql.Connection conn = MySQLconnection.getConnection(dbfile);
			String query = "SELECT prgid, (SUM(click)/SUM(pv))*100 as ctr FROM report_adv_prg " +
					"where createtime >= '" + begindate + "' and createtime <= '" + enddate + "' GROUP BY prgid";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			int count = 0;
			while(rs.next())
			{
				StringBuilder sb = new StringBuilder();

				String prgid = rs.getString(1);
				String ctr = rs.getString(2);
				//System.out.println(prgid + "||" + ctr);
				
				sb.append(prgid).append(",").append(ctr);
			
				JMSsender("adwo.update_app_ctr", sb.toString(), 0);
				//System.out.println(count);
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public void JMSsender(String QueueName, String msg, int showswitch) {
		ConnectionFactory connectionFactory;

		Connection connection = null;

		Session session;

		Destination destination;

		MessageProducer producer;

		connectionFactory = new ActiveMQConnectionFactory(
				ActiveMQConnection.DEFAULT_USER,
				ActiveMQConnection.DEFAULT_PASSWORD,
				"tcp://192.168.1.225:61616");

		try {
			connection = connectionFactory.createConnection();

			connection.start();

			session = connection.createSession(Boolean.TRUE,
					Session.AUTO_ACKNOWLEDGE);

			destination = session.createQueue(QueueName);

			producer = session.createProducer(destination);

			sendMessage(session, producer, msg, showswitch);

			session.commit();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != connection)
					connection.close();
			} catch (Throwable ignore) {
			}
		}
	}

	public void sendMessage(Session session, MessageProducer producer, String msg, int showswitch)
			throws Exception {
		MapMessage mapMessage = session.createMapMessage();
		
		//String[] details = msg.split("\\|");
		//System.out.println(details.length);
		//for(int i = 0; i < details.length; i++)
		//{
		//	System.out.println("Posting " + i + " data to queue: " + details[i] );
			mapMessage.setString("resultData", msg);
			producer.send(mapMessage);
		//}
	}
}
