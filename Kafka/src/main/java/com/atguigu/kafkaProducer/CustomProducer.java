package com.atguigu.kafkaProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author CZY
 * @date 2021/11/3 14:45
 * @description CustomProducer
 */
public class CustomProducer {
    public static void main(String[] args) {
        //2.根据第一步中需要传进的Properties信息，直接创建一个
        Properties properties = new Properties();
        //3.给kafka配置对象添加配置对象
        properties.put("bootstrap.servers","hadoop102:9092");

        // key,value序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //1.直接调用创建构建好KafkaProducer对象
        KafkaProducer<String, String> kfkPro = new KafkaProducer<>(properties);

        //4.在main线程里使用send发送ProducerRecord
        for (int i = 0; i < 10; i++) {
            kfkPro.send(new ProducerRecord<String, String>("first","kafka"+i));
        }
        //5.关闭资源
        kfkPro.close();

    }
}
