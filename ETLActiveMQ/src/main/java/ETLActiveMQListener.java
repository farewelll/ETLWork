import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

public class ETLActiveMQListener implements MessageListener {

    public void onMessage(Message message) {


        try {
            TextMessage map = (TextMessage) message;
            String text = map.getText();
            message.acknowledge();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}