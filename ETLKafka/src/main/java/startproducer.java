import java.util.Map;

public class startproducer {
    public static void main(String []args){
        System.out.println("kafka producer start!");
        ETLKafkaProducer producer = new ETLKafkaProducer("nameserver","topic");
        producer.instantiation();
        producer.initTransactions();
        //kafkaConsumer();
        System.out.println("kafka producer end!");
    }
}