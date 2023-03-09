package com.example.demo;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.*;

@SpringBootApplication
public class Demo1Application {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(Demo1Application.class, args);
//        Properties properties = new Properties();
////        properties.put("bootstrap.servers", "111.56.16.62:631");
//        properties.put("bootstrap.servers", "81.68.219.199:9092");
//        properties.put("connections.max.idle.ms", 10000);
//        properties.put("request.timeout.ms", 5000);
//        properties.put("group.id", "metric-group");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset","earliest");
////        Properties props = new Properties();
////        props.put("bootstrap.servers", "101.35.166.55:9092");
////        props.put("acks", "all"); props.put("retries", 0);
////        props.put("batch.size", 16384); props.put("linger.ms", 1);
////        props.put("group.id", "metric-group");
////        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
////        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
////        consumer.subscribe(Arrays.asList("speed"));
////        ConsumerRecords<String, String> records = consumer.poll(100);
//        String topicName = "speed";
////用于分配topic和partition
//        consumer.assign(Arrays.asList(new TopicPartition(topicName, 0)));
////不改变当前offset，指定从这个topic和partition的开始位置获取。
//        consumer.seekToBeginning(Arrays.asList(new TopicPartition(topicName, 0)));
//
//        for (int i = 0; i < 1; i++) {
//            ConsumerRecords<String, String> records = consumer.poll(10000);
//
//            System.out.printf("records length = %s%n", records.count());
////            Thread.sleep(1000);
//            int j = 0;
//            for (ConsumerRecord record : records) {
//                System.out.printf("topic = %s, partition = %s, offset = %s, key = %s, value = %s%n",
//                        record.topic(), record.partition(), record.offset(),
//                        record.key(), record.value());
//                JSONObject jsonObject = JSONObject.parseObject(record.value().toString());
//                System.out.println("result:"+jsonObject.getInnerMap().keySet().getClass());
//                j++;
//                if(j > 10){
//                    break;
//                }
//            }
//        }

        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "81.68.219.199:9092");
        properties.put("bootstrap.servers", "111.56.16.62:631");
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try (AdminClient client = KafkaAdminClient.create(properties)) {
            ListTopicsResult topics = client.listTopics(new ListTopicsOptions().timeoutMs(5000));
            Set<String> names = topics.names().get();
            System.out.println("connect to kafka cluster success");
        } catch (Exception e){
            System.out.println(e);
        }

//        while (true) {
//            for (ConsumerRecord<String, String> record : records)
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());}
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(100);
//            for (ConsumerRecord<String, String> record : records)
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());}
//        try (AdminClient client = KafkaAdminClient.create(properties)) {
//            ListTopicsResult topics = client.listTopics(new ListTopicsOptions().timeoutMs(5000));
//            Set<String> names = topics.names().get();
//            System.out.println("names:"+names);
//            System.out.println("connect to kafka cluster success");
//        } catch (Exception e){
//            System.out.println("failed");
//        }
    }


//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "101.35.166.55:9092");
//        properties.put("connections.max.idle.ms", 10000);
//        properties.put("request.timeout.ms", 5000);
//        try (AdminClient client = KafkaAdminClient.create(properties)) {
//            ListTopicsResult topics = client.listTopics(new ListTopicsOptions().timeoutMs(5000));
//            Set<String> names = topics.names().get();
//            System.out.println("connect to kafka cluster success");
//            return true;
//        } catch (Exception e){
//            return false;
//        }

}
