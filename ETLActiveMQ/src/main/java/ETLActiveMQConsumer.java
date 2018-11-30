import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;


public class ETLActiveMQConsumer {
    //tcp link
    public static final String BROKER_URL = "tcp://host-link";
    //resource topic
    public static final String DESTINATION = "activemq-topic";


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
            //establish a consumer
            MessageConsumer consumer = session.createConsumer(topic);
            System.out.println("Comsumer of topic is ready");
            consumer.setMessageListener(new ETLActiveMQListener());
            session.commit();
            System.in.read();
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
            ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
            //connect to ActiveMQ
            connection = ((ActiveMQConnectionFactory) factory).createQueueConnection();
            //start!
            connection.start();
            //establish session
            session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
            //establish Queue
            Queue queue = session.createQueue(DESTINATION);
            //establish consumer
            MessageConsumer consumer = session.createConsumer(queue);
            // establish listener
            System.out.println("Comsumer of topic is ready");
            consumer.setMessageListener(new ETLActiveMQListener());
            session.commit();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (session != null) {
                session.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        ETLActiveMQConsumer.runTopicFunction();

    }
}