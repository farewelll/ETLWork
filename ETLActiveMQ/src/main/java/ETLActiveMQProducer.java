import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;


public class ETLActiveMQProducer {
    //
    //tcp link
    public static final String BROKER_URL = "tcp://host-link";
    //resource topic
    public static final String DESTINATION = "activemq-topic";

    public static void sendMsg2Topic(TopicSession session, TopicPublisher publisher) throws Exception {
        for (int i = 0; i < 10; i++) {
            //send massage to ActiveMQ!
            String message2 = "test";
            TextMessage map = session.createTextMessage(message2);
            publisher.send(map);
        }
    }


    public static void sendMsg2Queue(Session session, MessageProducer producer) throws Exception {
        for (int i = 0; i < 10; i++) {
            //send massage to ActiveMQ!
            String message = "This is test message and count is " + i;
            MapMessage map = session.createMapMessage();
            map.setString("test", message);
            map.setLong("time", System.currentTimeMillis());
            System.out.println("Sending massage " + i);
            producer.send(map);
        }
    }


    public static void runTopicFunction() throws Exception {
        TopicConnection connection = null;
        TopicSession session = null;
        try {
            //connect to factory
            TopicConnectionFactory factory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER, ActiveMQConnection.DEFAULT_PASSWORD, BROKER_URL);
            connection = factory.createTopicConnection();
            //start!
            connection.start();
            //establish a session
            session = connection.createTopicSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            //establish a topic
            Topic topic = session.createTopic(DESTINATION);
            //establish a publisher
            TopicPublisher publisher = session.createPublisher(topic);
            //Persistence
            publisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            sendMsg2Topic(session, publisher);
            session.commit();
        } catch (Exception e) {
            throw e;
        } finally {
            //release source
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }


    public static void runQueueFunction() throws Exception {
        Connection connection = null;
        Session session = null;
        try {
            //establish factory
            ConnectionFactory factory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER, ActiveMQConnection.DEFAULT_PASSWORD, BROKER_URL);
            //connect to ActiveMQ
            connection = ((ActiveMQConnectionFactory) factory).createQueueConnection();
            //start!
            connection.start();
            //establish a session
            session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            //establish a Queue
            Queue queue = session.createQueue(DESTINATION);
            //establish a producer
            MessageProducer producer = session.createProducer(queue);
            //Persistence
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            sendMsg2Queue(session, producer);
            session.commit();
        } catch (Exception e) {
            throw e;
        } finally {
            //release source
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        while (true) {
            ETLActiveMQProducer.runTopicFunction();
            System.out.println("Producer is running");
        }

    }
}