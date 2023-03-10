package com.example.demo;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class CustomConsumer {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "81.68.219.199:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 创建消费者组，组名任意起名都可以
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-3");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1000000");

        Connection conn = null;
        PreparedStatement ps = null;

        // 创建消费者
        KafkaConsumer<String, String> consumer0 = new KafkaConsumer<String, String>(properties);

        // 订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("cumtcsb617-v-device-emf1");
        consumer0.subscribe(topics);
//        consumer1.subscribe(topics);
//        consumer2.subscribe(topics);

        //redis存值
//        Jedis jedis = new Jedis("81.68.219.199",6379);
//        jedis.auth("lizishudd");
//        jedis.select(4);


//        jedis.set("offset0","0");
//        jedis.set("offset1","0");
//        jedis.set("offset2","0");

        //对字符串的操作
        //存储一个值
//        String s = jedis.set("k1","v1");
//        System.out.println("返回的结果:"+jedis.get("k1"));


        // 获取所有的分区信息
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            // 获取消费者分区分配信息（有了分区分配信息才能开始消费）
            consumer0.poll(Duration.ofSeconds(1));
            assignment = consumer0.assignment();

        }

        System.out.println("11111111111" + assignment);

        int count = 0 ;

        for (TopicPartition tp : assignment) {
            conn = JDBCUtils.getConnection();
            String sql = "insert into nonflink_cumtcsb617_v_device_emf1(productId, createTime, negativeTotalInt, flowRate, id, deviceId, flow, positiveTotalInt, timestamp)values(?, ?, ?, ?, ?, ?, ?, ?, ?)";
            ps = conn.prepareStatement(sql);
            System.out.println("1111111111111"+tp);
            consumer0.seek(tp, 0);
            System.out.println("xxxxxxxxxxx" + tp);
            ConsumerRecords<String, String> consumerRecords = consumer0.poll(Duration.ofSeconds(100));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
                JSONObject jsonObject = JSONObject.parseObject(consumerRecord.value());
                if(jsonObject.get("type") == null){
                    System.out.println("111111111111"+jsonObject);
                    ps.setString(1,jsonObject.getString("productId"));
                    ps.setString(2, jsonObject.getString("createTime"));
                    ps.setInt(3, Integer.parseInt(jsonObject.getString("negativeTotalInt")));
                    ps.setDouble(4, Double.parseDouble(jsonObject.getString("flowRate")));
                    ps.setString(5,jsonObject.getString("id"));
                    ps.setString(6,jsonObject.getString("deviceId"));
                    ps.setDouble(7, Double.parseDouble(jsonObject.getString("flow")));
                    ps.setInt(8, Integer.parseInt(jsonObject.getString("positiveTotalInt")));
                    ps.setString(9, jsonObject.getString("timestamp"));
                    System.out.println("111111111111"+ps);
                    ps.execute();
                }
                count++;
                if(count == 10){
                    JDBCUtils.closeResource(conn,ps);
                    break;
                }
            }
        }



//        // 遍历所有分区，并指定 offset 从55的位置开始消费
//        for (TopicPartition tp : assignment) {
//            int tpa = 0;
//            int tpb = 0;
//            int tpc = 0;
//            if(tp.toString().equals("xyl-0")){
//                consumer0.seek(tp, Long.parseLong(jedis.get("offset0")));
//                System.out.println("xxxxxxxxxxx" + tp);
//                ConsumerRecords<String, String> consumerRecords = consumer0.poll(Duration.ofSeconds(100));
//                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
//                    System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
//                    tpa++;
//                    if(tpa == 10){
//                        jedis.set("offset0", String.valueOf(consumerRecord.offset()));
//                        break;
//                    }
//                }
//            }
//            if(tp.toString().equals("xyl-1")){
//                consumer0.seek(tp, Long.parseLong(jedis.get("offset1")));
//                System.out.println("xxxxxxxxxxx" + tp);
//                ConsumerRecords<String, String> consumerRecords = consumer0.poll(Duration.ofSeconds(100));
//                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
//                    System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
//                    tpb++;
//                    if(tpb == 10){
//                        jedis.set("offset1", String.valueOf(consumerRecord.offset()));
//                        break;
//                    }
//                }
//            }
//            if(tp.toString().equals("xyl-2")) {
//                consumer0.seek(tp, Long.parseLong(jedis.get("offset2")));
//                System.out.println("xxxxxxxxxxx" + tp);
//                ConsumerRecords<String, String> consumerRecords = consumer0.poll(Duration.ofSeconds(100));
//                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
//                    System.out.println("分区："+consumerRecord.partition()+",消息："+consumerRecord.value()+",offset："+consumerRecord.offset());
//                    tpc++;
//                    if(tpc == 10){
//                        jedis.set("offset2", String.valueOf(consumerRecord.offset()));
//                        break;
//                    }
//                }
//            }
//
//        }


    }
}
