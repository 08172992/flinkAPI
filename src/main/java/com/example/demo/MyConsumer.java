package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    private final static String TOPIC_NAME="xyl";
    private final static String CONSUMER_GROUP_NAME="testGroup";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"81.68.219.199:9092");
        /**
         * 1.消费分组名
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,CONSUMER_GROUP_NAME);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"0");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        /**
         * 1.2设置序列化
         */
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        /**
         * 2.创建一个消费者的客户端
         */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        /**
         * 3.消费者订阅主题列表
         */
        consumer.subscribe(Arrays.asList(TOPIC_NAME));


        for(int i = 0 ; i < 10; i++){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("收到的消息：partition= %d,offset= %d,key= %s,value=%s %n",record.partition(),
                        record.offset(),record.key(),record.value());
            }
        }
    }
}
