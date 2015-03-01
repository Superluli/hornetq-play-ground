package com.superluli.hornetq.jms;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
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

public class Server {

	private static EmbeddedJMS SERVER;

	public static void main(String[] args) throws Exception {
		startServer();
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
		JMSQueueConfiguration queueConfig = new JMSQueueConfigurationImpl("test_queue", null, true);
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
