package de.quoss.mq.client;

import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import java.util.Arrays;
import java.util.Enumeration;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    public static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    
    final String channelValue;
    final String functionValue;
    final String hostNameValue;
    final String passwordValue;
    final String portValue;
    final String queueValue;
    final String queueManagerValue;
    final String typeValue;
    final String useridValue;
    
    public App(String[] args) {
        LOGGER.trace("init start [args={}]", Arrays.asList(args));
        Options options = new Options();
        Option channelOption = Option.builder("c")
                .longOpt("channel")
                .hasArg()
                .required()
                .build();
        Option functionOption = Option.builder("fn")
                .longOpt("function")
                .hasArg()
                .required()
                .build();
        Option hostNameOption = Option.builder("h")
                .longOpt("host-name")
                .hasArg()
                .build();
        Option passwordOption = Option.builder("pw")
                .longOpt("password")
                .hasArg()
                .required()
                .build();
        Option portOption = Option.builder("p")
                .longOpt("port")
                .hasArg()
                .build();
        Option queueOption = Option.builder("q")
                .longOpt("queue")
                .hasArg()
                .required()
                .build();
        Option queueManagerOption = Option.builder("qm")
                .longOpt("queue-manager")
                .hasArg()
                .required()
                .build();
        Option typeOption = Option.builder("t")
                .longOpt("type")
                .hasArg()
                .required()
                .build();
        Option useridOption = Option.builder("u")
                .longOpt("userid")
                .hasArg()
                .required()
                .build();
        options.addOption(channelOption);
        options.addOption(functionOption);
        options.addOption(hostNameOption);
        options.addOption(passwordOption);
        options.addOption(portOption);
        options.addOption(queueOption);
        options.addOption(queueManagerOption);
        options.addOption(typeOption);
        options.addOption(useridOption);
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            throw new IllegalStateException("Error parsing command line: " + e.getMessage(), e);
        }
        channelValue = commandLine.getOptionValue("c");
        functionValue = commandLine.getOptionValue("fn");
        hostNameValue = commandLine.getOptionValue("h");
        passwordValue = commandLine.getOptionValue("pw");
        portValue = commandLine.getOptionValue("p");
        queueValue = commandLine.getOptionValue("q");
        queueManagerValue = commandLine.getOptionValue("qm");
        typeValue = commandLine.getOptionValue("t");
        useridValue = commandLine.getOptionValue("u");
        LOGGER.trace("init end");
    }
    
    public static void main(String[] args) {
        LOGGER.trace("main start");
        new App(args).run();
        LOGGER.trace("main end");
    }

    private Connection connection(ConnectionFactory factory) throws JMSException {
        LOGGER.trace("connection start");
        Connection result;
        if (passwordValue == null || useridValue == null) {
            throw new IllegalStateException("Invalid credentials provided");
        }
        result = factory.createConnection(useridValue, passwordValue);
        LOGGER.trace("connection end [result={}]", result);
        return result;
    }

    private ConnectionFactory connectionFactory() {
        LOGGER.trace("connection-factory start");
        MQQueueConnectionFactory result = new MQQueueConnectionFactory();
        try {
            result.setChannel(channelValue);
            if (hostNameValue != null) {
                result.setHostName(hostNameValue);
            }
            if (portValue != null) {
                result.setPort(Integer.parseInt(portValue));
            }
            result.setQueueManager(queueManagerValue);
            result.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        } catch (JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        LOGGER.trace("connection-factory end [result={}]", result);
        return result;
    }

    private void listMessageIds() {
        LOGGER.trace("list-message-ids start");
        if (null == typeValue) {
            throw new IllegalStateException("No type value provided");
        } else {
            switch (typeValue) {
                case "jms":
                    listMessageIdsJms();
                    break;
                default:
                    throw new IllegalStateException("Type value '" + typeValue + "' not supported");
            }
        }
        LOGGER.trace("list-message-ids end");
    }

    private void listMessageIdsJms() {
        LOGGER.trace("list-message-ids-jms start");
        ConnectionFactory factory = connectionFactory();
        try (Connection connection = connection(factory);
             Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE)) {
            LOGGER.debug("list-message-ids-jms [connection={},session={}]", connection, session);
            Queue queue = session.createQueue(queueValue);
            QueueBrowser browser = session.createBrowser(queue);
            Enumeration<?> enumeration = browser.getEnumeration();
            int i = 0;
            while (enumeration.hasMoreElements()) {
                Object o = enumeration.nextElement();
                LOGGER.debug("list-message-ids-jms [o.class.name={},o={}]", o.getClass().getName(), o);
                i++;
            }
            LOGGER.debug("list-message-ids-jms [i={}]", i);
            browser.close();
        } catch (JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        LOGGER.trace("list-message-ids-jms end");
    }

    private void putMessage() {
        LOGGER.trace("put-message start");
        if (null == typeValue) {
            throw new IllegalStateException("No type value provided");
        } else {
            switch (typeValue) {
                case "jms":
                    putMessageJms();
                    break;
                default:
                    throw new IllegalStateException("Type value '" + typeValue + "' not supported");
            }
        }
        LOGGER.trace("put-message end");
    }

    private void putMessageJms() {
        LOGGER.trace("put-message-jms start");
        ConnectionFactory factory = connectionFactory();
        try (Connection connection = connection(factory);
             Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE)) {
            LOGGER.debug("put-message-jms [connection={},session={}]", connection, session);
            Queue queue = session.createQueue(queueValue);
            TextMessage message = session.createTextMessage();
            message.setText("foo");
            try (MessageProducer producer = session.createProducer(queue)) {
                producer.send(message);
            }
            session.commit();
        } catch (JMSException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        LOGGER.trace("put-message-jms end");
    }

    private void run() {
        LOGGER.trace("run start");
        if (null == functionValue) {
            throw new IllegalStateException("No function value provided");
        } else {
            switch (functionValue) {
                case "list-message-ids":
                    listMessageIds();
                    break;
                case "put-message":
                    putMessage();
                    break;
                default:
                    throw new IllegalStateException("Function value '" + functionValue + "' not supported");
            }
        }
        LOGGER.trace("run end");
    }

}
