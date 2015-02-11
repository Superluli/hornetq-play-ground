package com.superluli.hornetq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;

public class Example {

	public static void main(String[] args) throws Exception{
		System.out.println("Hornetq Test");
		// TODO Auto-generated method stub
		TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
		ConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,transportConfiguration);
		Queue queue = HornetQJMSClient.createQueue("OrderQueue");
		Connection connection = cf.createConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = session.createProducer(queue);
		MessageConsumer consumer = session.createConsumer(queue);
		connection.start();
		
		TextMessage message = session.createTextMessage("xxx");
		producer.send(message);
		System.out.println(consumer.receive().getBody(String.class));
	}

}
