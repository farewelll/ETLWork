import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.lang.reflect.Array;
import java.util.*;

public class ETLKafkaConsumer {
    //kafka basic information configuration
    String NameServer = null;
    Properties properties;
    KafkaConsumer<Object, Object> consumer = null;
    String Topic;
    String ETLConsumer = "Kafka";
    //configuration !
    public ETLKafkaConsumer(String NameServer, String Topic) {
        //set NameServer
        this.NameServer = NameServer;
        //set Topic that you want to subscribe
        this.Topic = Topic;
        //set properties
        this.properties = setKafkaConsumerProperties(NameServer);
    }
    //initialization kafka !
    public void initialization() {
        //instantiation a kafka consumer
        System.out.println("initialization!");
        consumer = new KafkaConsumer<Object, Object>(properties);
    }
    public Properties setKafkaConsumerProperties(String NameServer){
        Properties props = new Properties();
        props.put("bootstrap.servers", NameServer);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
    public void getDataFromKafka(){
        consumer.subscribe(Arrays.asList(Topic));
        //get and store data from kafka ,it is 24/7 working
        Map<Object , Object> data = new HashMap<Object, Object>();
        long updatecount = 0;
        try {
            while(true){
                long number = 900;
                ConsumerRecords<Object , Object> records = consumer.poll(number);

                for (ConsumerRecord<Object , Object> record:records){
                    String topic = record.topic();
                    int partition = record.partition();
                    String key = (String) record.key();
                    String value = (String) record.value();
                    Long offset = record.offset();
                    System.out.println("record = " + record);
                    System.out.println(topic + " " + partition + " " + key + " " + value + " " + " offset = " + offset);
                    data.put("topic" , record.topic());
                    data.put("partition" , record.partition());
                    data.put("key" , (String) record.key());
                    data.put("value" ,(String) record.value() );
                    data.put("offset" ,record.offset());
                    updatecount ++;
                }
                consumer.commitAsync();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }


}
