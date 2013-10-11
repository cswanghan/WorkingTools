/**
 * 
 */
package com.adwo.GetAndroidEffect;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @author dev
 * 
 */
public class GetDailyAdEffect {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		GetDailyAdEffect gae = new GetDailyAdEffect();
		if(args.length < 3 || args.length > 3)
		{
			System.out.println("Wrong Input Param, Correct Format: java -jar XXX.jar <input_file> <queue_name> <show_switch>");
		}
		else if(args.length == 3)
			gae.GetAdActive(args[0],args[1],Integer.valueOf(args[2]));
	}

	public void GetAdActive(String input, String queuename, int showswitch) throws Exception {
		InputStreamReader InputFile = new InputStreamReader(
				new FileInputStream(input), "UTF-8");
		BufferedReader FileReader = new BufferedReader(InputFile);
		String temp = null;
		HashMap<String, Integer> temp_map = new HashMap<String, Integer>();
		while ((temp = FileReader.readLine()) != null) {
			String[] details = temp.split(",");
			String ad_id = details[3];
			String date = details[14];
			String combinedkey = ad_id + ":" + date;
			if (!temp_map.containsKey(combinedkey))
				temp_map.put(combinedkey, 1);
			else {
				int tempcount = temp_map.get(combinedkey);
				tempcount++;
				temp_map.put(combinedkey, tempcount);
			}
		}

		StringBuilder sb = new StringBuilder();
		int count = 0; 
		Iterator<String> map_iter = temp_map.keySet().iterator();
		while (map_iter.hasNext()) {
			String key = map_iter.next();
			String[] key_details = key.split(":");
			String ad_id = key_details[0];
			String date = key_details[1];
			int value = temp_map.get(key);
			if(showswitch == 1)
				System.out.println(ad_id + ":" + date + ":" + value);
			
			if(count == 0)
				sb.append(ad_id + ":" + date + ":" + value);
			else
				sb.append("|").append(ad_id + ":" + date + ":" + value);
			count++;
		}
		
		//JMSsender("adwo.adv_active", sb.toString());
		JMSsender(queuename, sb.toString(), showswitch);
	}

	public void JMSsender(String QueueName, String msg, int showswitch) {
		ConnectionFactory connectionFactory;

		Connection connection = null;

		Session session;

		Destination destination;// Destination ：消息的目的�?

		MessageProducer producer;// MessageProducer：消息生产�?

		// TextMessage message;

		connectionFactory = new ActiveMQConnectionFactory(
				ActiveMQConnection.DEFAULT_USER,
				ActiveMQConnection.DEFAULT_PASSWORD,
				"tcp://119.161.183.203:6666");

		try {
			// 构�?从工厂得到连接对�?
			connection = connectionFactory.createConnection();

			// 启动
			connection.start();

			// 获取操作连接
			session = connection.createSession(Boolean.TRUE,
					Session.AUTO_ACKNOWLEDGE);

			// 创建队列
			destination = session.createQueue(QueueName);

			// 得到消息生产�?
			producer = session.createProducer(destination);

			// 设置不持久化
			// producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

			// 构�?消息
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
		
		String[] details = msg.split("\\|");
		for(int i = 0; i < details.length; i++)
		{
			if(showswitch == 1)
				System.out.println(details[i]);
			String[] deepper_details = details[i].split(":");
			if(showswitch == 1)
				System.out.println("Posting data to queue: " + deepper_details[0] + ":" + deepper_details[1] + ":" + deepper_details[2]);
			mapMessage.setString("advid", deepper_details[0]);
			mapMessage.setString("date", deepper_details[1]);
			mapMessage.setString("count", deepper_details[2]);
			producer.send(mapMessage);
		}
	}
}
