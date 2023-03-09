package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class CustomConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "81.68.219.199:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 创建消费者组，组名任意起名都可以
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-3");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("xyl");
        consumer.subscribe(topics);

        // 获取所有的分区信息
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofSeconds(1));
            // 获取消费者分区分配信息（有了分区分配信息才能开始消费）
            assignment = consumer.assignment();
        }

        System.out.println("11111111111" + assignment);


        // 遍历所有分区，并指定 offset 从55的位置开始消费
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 0);
            System.out.println("xxxxxxxxxxx" + tp);
//            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
//            for (ConsumerRecord<String, String> consumerRecord : consumerRecords){
//                System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
////                if (consumerRecord.partition() == 2){
////                    System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
////                }
//            }
        }
        int flag = 0;
        boolean flag1 = true;
        int tpa = 0;
        int tpb = 0;
        int tpc = 0;
        Long osa;
        long osb;
        long osc;

        // 消费数据
        while (flag1) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                if (consumerRecord.partition() == 0){
                    System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
                    if(tpa == 9){
                        break;
                    }
                    tpa++;
                }
                if(consumerRecord.partition() == 1){
                    System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
                    if(tpb == 9){
                        break;
                    }
                    tpb++;
                }
                if ((consumerRecord.partition() == 2) && (consumerRecord.offset() < 10L) ){
                    System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
                    if(tpc == 9){
                        break;
                    }
                    System.out.println("hhhhhhhhhhhhh"+tpc);
                    tpc++;
                }

//                System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
//                flag++;
//                System.out.println("hhhhhhhh："+flag);
//
//                if(flag == 1000){
//                    flag1 = false;
//                    break;
//                }



                if((tpa == 9) && (tpb == 9) && (tpc == 9)) {
                    flag1 = false;
                    break;
                }

            }
        }
    }
}
