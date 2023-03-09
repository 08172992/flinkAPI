package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


public class ConsumerDemo {

    private final static AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        Properties props = consumerConfigs();

        KafkaConsumer<String, String> stringStringKafkaConsumer = new KafkaConsumer<>(props);
        stringStringKafkaConsumer.subscribe(Arrays.asList("xyl"));

        while (isRunning.get()) {
            try {
                ConsumerRecords<String, String> poll = stringStringKafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> stringStringConsumerRecord : poll) {
                    System.out.println("offset:"+stringStringConsumerRecord.value());
                }
//                for (record : poll) {
//
//                }
            } catch (Exception e) {
//                log.error("消费者消费过程发生错误", e);

            }
        }

        stringStringKafkaConsumer.close();
    }

    public static Properties consumerConfigs() {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "81.68.219.199:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer.01");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.01");
        return props;
    }
}
