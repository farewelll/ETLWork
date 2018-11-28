import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class ETLRocketMQCommer {
    public static boolean AsyncConsumer(String Nameserver ,String Topic){
        // initialization consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ETLConsumer");
        //set nameserver eg:x.x.x.x:xxxx
        consumer.setNamesrvAddr(Nameserver);
        //set offset
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //set model broadcast/cluster
        consumer.setMessageModel(MessageModel.BROADCASTING);
        //set max thread
        consumer.setConsumeThreadMax(30);
        //set max batch size
        consumer.setConsumeMessageBatchMaxSize(10);
        try {
            //set topic and tag . if not appoint tag ,please use "*"
            consumer.subscribe(Topic, "*");
            //set listener to handle received message
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                    System.out.println("Receive new message :" + Thread.currentThread().getName() +list);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("RocketMQ consummer or listener faild");
        }
        try {
            consumer.start();
        } catch (Exception e){
            e.printStackTrace();
            System.out.println("RocketMQ start faild");
        }

        return true;
    }
    public static void main(String []args){
        System.out.println("RocketMQ begin !");
        String NameServer = "server";
        String Topic = "Topic";
        AsyncConsumer(NameServer , Topic);
    }
}
