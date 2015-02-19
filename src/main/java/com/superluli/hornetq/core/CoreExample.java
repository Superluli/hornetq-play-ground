package com.superluli.hornetq.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientRequestor;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.embedded.EmbeddedHornetQ;

public class CoreExample {

	private static EmbeddedHornetQ SERVER;

	private static final String ADDRESS = "SomeAddress";
	private static final String QUEUE_NAME = "com.superluli.somequeue";
	private static final int NUM = 10000;
	
	public static void main(String[] args) throws Exception {

		startServer();
		ClientSession producerSession = createProducerSession();
		ClientSession consumerSession = createConsumerSession();

		producerSession.createQueue(ADDRESS, QUEUE_NAME, false);
		producerSession.setSendAcknowledgementHandler(new SendAcknowledgementHandler() {
			public void sendAcknowledged(Message message) {
			}
		});
		
		ClientProducer producer = producerSession.createProducer();
		
		consumerSession.start();
		
		ClientConsumer consumer = consumerSession.createConsumer(QUEUE_NAME);
		final AtomicInteger count = new AtomicInteger(0);
		consumer.setMessageHandler(new MessageHandler() {
			public void onMessage(ClientMessage message) {
				//System.out.print("received message : ");
				//System.out.println(message.getBodyBuffer().readString());
				count.incrementAndGet();
			}
		});		
		
		for (int i = 0; i < NUM; i++) {
			sendMessage(producerSession, producer);
		}
		
		//consumerSession.close();
		//Thread.sleep(1000);
		System.out.println(count.get());
		//stopSeraver();
	};

	public static ClientSession createProducerSession() throws Exception {
		Map<String, Object> connectionParams = new HashMap<String, Object>();
		connectionParams.put(
				org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, 5555);

		ServerLocator locator = HornetQClient
				.createServerLocator(false,
						new TransportConfiguration(NettyConnectorFactory.class.getName(),
								connectionParams));
		locator.setConfirmationWindowSize(1*1024);
		//locator.setBlockOnNonDurableSend(true);
		return locator.createSessionFactory().createSession(true, true);
	}

	public static ClientSession createConsumerSession() throws Exception {
		Map<String, Object> connectionParams = new HashMap<String, Object>();
		connectionParams.put(
				org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, 5555);

		ServerLocator locator = HornetQClient
				.createServerLocator(false,
						new TransportConfiguration(NettyConnectorFactory.class.getName(),
								connectionParams));
		locator.setBlockOnAcknowledge(false);
		ClientSessionFactory sessionFactory = locator.createSessionFactory();
		return sessionFactory.createSession(true, true);
	}
	
	public static void sendMessage(ClientSession session, ClientProducer producer) throws Exception {
		Message message = session.createMessage(false);
		message.getBodyBuffer().writeString(String.valueOf(new Random().nextInt(1000)));
		producer.send(new SimpleString(ADDRESS), message);
	}

	public static void manage(ClientSession session) throws Exception {
		ClientRequestor requestor = new ClientRequestor(session, "jms.queue.hornetq.management");
		ClientMessage opmessage = session.createMessage(false);
		ManagementHelper.putAttribute(opmessage, "core.queue." + QUEUE_NAME, "messageCount");
		ClientMessage reply = requestor.request(opmessage);
		System.out.println("There are " + ManagementHelper.getResult(reply) + " messages in "
				+ QUEUE_NAME);
	}

	public static void startServer() throws Exception {
		// TODO Auto-generated method stub
		Configuration config = new ConfigurationImpl();
		Map<String, Object> transportParams = new HashMap<String, Object>();
		transportParams.put(TransportConstants.HOST_PROP_NAME, "localhost");
		transportParams.put(TransportConstants.PORT_PROP_NAME, 5555);
		HashSet<TransportConfiguration> transports = new HashSet<TransportConfiguration>();
		transports.add(new TransportConfiguration(NettyAcceptorFactory.class.getName(),
				transportParams));
		config.setSecurityEnabled(false);
		config.setAcceptorConfigurations(transports);
		SERVER = new EmbeddedHornetQ();
		SERVER.setConfiguration(config);
		SERVER.start();
	}

	public static void stopSeraver() throws Exception {
		SERVER.stop();
	}
}
