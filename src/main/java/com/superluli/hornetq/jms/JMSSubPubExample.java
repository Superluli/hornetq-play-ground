package com.superluli.hornetq.jms;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.CompletionListener;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;

public class JMSSubPubExample {

	public static void main(String[] args) throws Exception {

		JMSContext consumerContext = createContext();
		Topic topic = consumerContext.createTemporaryTopic();
		
		JMSConsumer consumer1 = consumerContext.createConsumer(topic);
		JMSConsumer consumer2 = consumerContext.createSharedConsumer(topic, "subscription1");
		JMSConsumer consumer3 = consumerContext.createSharedConsumer(topic, "subscription1");
		
		registerConsumerListener(consumer1);
		registerConsumerListener(consumer2);
		registerConsumerListener(consumer3);
		
		produce(topic);
		
		Thread.sleep(5000);
		consumerContext.close();
	}

	public static void browser(Queue queue) throws Exception {
		JMSContext browserContext = createContext();
		QueueBrowser browser = browserContext.createBrowser(queue);
		Enumeration<?> messageEnum = browser.getEnumeration();
		while (messageEnum.hasMoreElements()) {
			TextMessage message = (TextMessage) messageEnum.nextElement();
			System.out.println("Browsing: " + message.getJMSMessageID());
		}
		browserContext.close();
	}

	public static void produce(Destination destination) throws Exception {
		JMSContext producerContext = createContext();
		JMSProducer producer = producerContext.createProducer();
		producer.setAsync(new CompletionListener() {

			public void onException(Message message, Exception exception) {
				exception.printStackTrace();
			}

			public void onCompletion(Message message) {
				try {
					//System.out.println("Producer sent message : " + message.getJMSMessageID());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		for (int i = 0; i < 2; i++) {
			TextMessage message = producerContext.createTextMessage("X");
			producer.send(destination, message);
		}
		producerContext.close();
	}

	public static void registerConsumerListener(final JMSConsumer consumer) throws Exception {
		consumer.setMessageListener(new MessageListener() {

			public void onMessage(Message message) {
				try {
					System.out.println(consumer + " received message : "
							+ message.getJMSMessageID());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	public static JMSContext createContext() throws Exception {
		Map<String, Object> transportParams = new HashMap<String, Object>();
		transportParams.put(TransportConstants.HOST_PROP_NAME, "localhost");
		transportParams.put(TransportConstants.PORT_PROP_NAME, 5555);

		ConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
				new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams));
		return cf.createContext(JMSContext.AUTO_ACKNOWLEDGE);
	}
}
