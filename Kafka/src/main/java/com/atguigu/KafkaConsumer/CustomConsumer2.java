package com.atguigu.KafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author CZY
 * @date 2021/11/5 10:14
 * @description CustomConsumer
 */
public class CustomConsumer2 {
    public static void main(String[] args) {
        // 1.创建消费者的配置对象
        Properties prop = new Properties();
        // 2.给消费者配置对象添加参数
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        // 配置反序列化 必须
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        // 配置消费者组 必须
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
       // prop.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.RoundRobinAssignor");

        //3.创建消费者对象
        KafkaConsumer<String, String> KfkCon = new KafkaConsumer<>(prop);

        // 注册主题
        ArrayList<String> strings = new ArrayList<>();
        strings.add("third");
        KfkCon.subscribe(strings);
        // 拉取数据打印
        while (true){
            ConsumerRecords<String, String> consumerRecords = KfkCon.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
