import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;


public class ETLKafkaProducer {
    //kafka basic information configuration
    String NameServer = null;
    Properties properties;
    KafkaProducer<Object, Object> producer = null;
    String Topic;
    String ETLProducer = "Kafka";

    public  ETLKafkaProducer(String NameServer , String Topic) {
        //set NameServer
        this.NameServer = NameServer;
        //set Topic that you want to subscribe
        this.Topic = Topic;
        //set properties
        this.properties = setKafkaProducerProperties(NameServer);
    }
    public Properties setKafkaProducerProperties(String NameServer){
        Properties props = new Properties();
        props.put("bootstrap.servers", NameServer);
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 300000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("transactional.id", "kafka001");
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return  props;
    }
    public void instantiation() {
        //instantiation kafka producer
        System.out.println("instantiation!");
        producer = new KafkaProducer<Object, Object>(properties);
    }
    public  void initTransactions() {
        producer.initTransactions();
        System.out.println("initTransaction success");
    }
    public void pushDataToKafka(String Key , String Value){
        //this is msg needed to be send
        ProducerRecord<Object,Object> record = new ProducerRecord<Object, Object>(Topic , 0,Key,Value);
        try {
            producer.beginTransaction();
            System.out.println("beginTransaction success");
            try{
                //send msg to kafka!
                RecordMetadata recordMetadata =  producer.send(record).get();
                System.out.println("Send msg success" + recordMetadata);
            }catch (Exception e ){
                e.printStackTrace();
                System.out.println("Send msg failed ! The msg is :" + record);
            }
        }catch (ProducerFencedException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            e.getCause();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
    }
}