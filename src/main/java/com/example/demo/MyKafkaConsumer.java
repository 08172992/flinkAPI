package com.example.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;



public class MyKafkaConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        // kafka地址
        props.put("bootstrap.servers", "81.68.219.199:9092");
        // 设置消费组
        props.put("group.id", "bigdata");
        // 是否自动提交
        props.put("enable.auto.commit", "true");
        // 设置自动提交时间隔
        props.put("auto.commit.interval.ms", "1000");
        // 设置消费,一般设置earliest或者latest
        props.put("auto.offset.reset", "earliest");
        // 序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("xyl"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
//                        record.offset(), record.key(), record.value());

                System.out.printf("收到的消息：partition= %d,offset= %d,key= %s,value=%s %n",record.partition(),
                        record.offset(),record.key(),record.value());
//                int partition = record.partition();
//                long offset = record.offset();
//                String key = record.key();
//                String value = record.value();
                // 打印消费参数
//                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", partition, offset, key, value);

            }
            consumer.commitAsync();
        }
    }

}
