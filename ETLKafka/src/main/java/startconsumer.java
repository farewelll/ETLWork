import java.util.Map;

public class startconsumer {
    public static void main(String []args){
        System.out.println("kafka consumer start!");
        ETLKafkaConsumer consumer = new ETLKafkaConsumer("nameserver","topic");
        consumer.initialization();
        consumer.getDataFromKafka();
        //kafkaConsumer();
        System.out.println("kafka consumer end!");
    }
}
