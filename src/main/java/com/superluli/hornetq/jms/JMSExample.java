package com.superluli.hornetq.jms;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.JMSQueueConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.hornetq.jms.server.embedded.EmbeddedJMS;

public class JMSExample {

	private static EmbeddedJMS SERVER;

	public static void main(String[] args) throws Exception {

		startServer();

		
		Queue queue = HornetQJMSClient.createQueue("queue1");
		
		produce(queue);
		browser(queue);
		
		Connection consumerConnection = consume(queue);
		Thread.sleep(2000);
		consumerConnection.close();
		stopServer();
	}

	public static void browser(Queue queue) throws Exception{
		Connection browserConnection = createBrowserConnection();
		Session browserSession = browserConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueBrowser browser = browserSession.createBrowser(queue);
        Enumeration<?> messageEnum = browser.getEnumeration();
        while (messageEnum.hasMoreElements())
        {
           TextMessage message = (TextMessage)messageEnum.nextElement();
           System.out.println("Browsing: " + message.getText());
        }
        browserConnection.close();
	}
	
	public static void produce(Queue queue) throws Exception {
		Connection producerConnection = createProducerConnection();
		Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = producerSession.createProducer(queue);
		for (int i = 0; i < 5; i++) {
			producer.send(
					producerSession.createTextMessage(String.valueOf(new Random().nextInt(10000))),
					new CompletionListener() {

						public void onException(Message message, Exception exception) {
							exception.printStackTrace();
						}

						public void onCompletion(Message message) {
							try {
								System.out.println("Producer sent message : "
										+ message.getBody(String.class));
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					});
		}
		producerConnection.close();
	}

	public static Connection consume(Queue queue) throws Exception {
		Connection consumerConnection = createConsumerConnection();
		consumerConnection.start();
		Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageConsumer consumer = consumerSession.createConsumer(queue);
		consumer.setMessageListener(new MessageListener() {

			public void onMessage(Message message) {
				try {
					System.out.println("Consumer received message : "
							+ message.getBody(String.class));
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		});
		return consumerConnection;
	}

	public static Connection createBrowserConnection() throws Exception {
		Map<String, Object> transportParams = new HashMap<String, Object>();
		transportParams.put(TransportConstants.HOST_PROP_NAME, "localhost");
		transportParams.put(TransportConstants.PORT_PROP_NAME, 5555);

		ConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
				new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams));

		return cf.createConnection();
	}
	
	public static Connection createProducerConnection() throws Exception {

		Map<String, Object> transportParams = new HashMap<String, Object>();
		transportParams.put(TransportConstants.HOST_PROP_NAME, "localhost");
		transportParams.put(TransportConstants.PORT_PROP_NAME, 5555);

		ConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
				new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams));

		return cf.createConnection();
	}

	public static Connection createConsumerConnection() throws Exception {

		Map<String, Object> transportParams = new HashMap<String, Object>();
		transportParams.put(TransportConstants.HOST_PROP_NAME, "localhost");
		transportParams.put(TransportConstants.PORT_PROP_NAME, 5555);

		ConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
				new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams));

		return cf.createConnection();
	}

	public static void startServer() throws Exception {

		// configure core
		Configuration configuration = new ConfigurationImpl();

		Map<String, Object> transportParams = new HashMap<String, Object>();
		transportParams.put(TransportConstants.HOST_PROP_NAME, "localhost");
		transportParams.put(TransportConstants.PORT_PROP_NAME, 5555);

		configuration.getAcceptorConfigurations().add(
				new TransportConfiguration(NettyAcceptorFactory.class.getName(), transportParams));
		configuration.getConnectorConfigurations().put("connector",
				new TransportConfiguration(NettyConnectorFactory.class.getName()));

		configuration.setSecurityEnabled(false);

		// configure jms
		JMSConfiguration jmsConfig = new JMSConfigurationImpl();
		ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl("cf",
				false, Arrays.asList("connector"), "/cf");
		jmsConfig.getConnectionFactoryConfigurations().add(cfConfig);

		// configure the JMS Queue
		JMSQueueConfiguration queueConfig = new JMSQueueConfigurationImpl("queue1", null, true,
				"/queue/queue1");
		jmsConfig.getQueueConfigurations().add(queueConfig);

		// Step 5. Start the JMS Server using the HornetQ core server and the
		// JMS configuration
		SERVER = new EmbeddedJMS();
		SERVER.setConfiguration(configuration);
		SERVER.setJmsConfiguration(jmsConfig);
		SERVER.start();
	}

	public static void stopServer() throws Exception {
		SERVER.stop();
	}

}
