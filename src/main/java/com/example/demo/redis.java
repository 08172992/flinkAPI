package com.example.demo;

import redis.clients.jedis.Jedis;

public class redis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("81.68.219.199",6379);
        jedis.auth("lizishudd");
        //对字符串的操作
        //存储一个值
        String s = jedis.set("k1","v1");
        System.out.println("返回的结果:"+jedis.get("k1"));

    }
}
