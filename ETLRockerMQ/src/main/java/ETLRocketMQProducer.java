import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;


public class ETLRocketMQProducer {
    public static boolean AsyncProducer(String NameServer) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("ETLProfucer");
        producer.setNamesrvAddr(NameServer);
        producer.start();
        Message message = new Message(
                "Topic",
                "Tag",
                "Keys",
                ("000").getBytes()
        ) ;
        producer.send(message, new SendCallback() {

            public void onSuccess(SendResult sendResult) {
                System.out.printf("Send Msg success" , sendResult);
            }

            public void onException(Throwable throwable) {
                System.out.println("Send Msg fail");
                throwable.printStackTrace();
            }
        });
        SendResult result = producer.send(message);
        System.out.println("result : " + result);
        producer.shutdown();

        return true;
    }

    public static void main(String []args){
        System.out.println("RocketMQ Producer start !");
        String NameServer = "server";
        try {
            AsyncProducer(NameServer);
        }catch (Exception e ){
            e.printStackTrace();
        }
    }
}
