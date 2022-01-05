package com.atguigu.kafkaProducer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 带回调的API
 * ProducerConfig：获取所需的一系列配置参数
 * @author CZY
 * @date 2021/11/3 14:45
 * @description CustomProducer
 */
public class CustomProducer2 {
    public static void main(String[] args) throws InterruptedException {
        //2.根据第一步中需要传进的Properties信息，直接创建一个
        Properties properties = new Properties();
        //3.给kafka配置对象添加配置对象
        //properties.put("bootstrap.servers","hadoop102:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        // key,value序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //1.直接调用创建构建好KafkaProducer对象
        KafkaProducer<String, String> kfkPro = new KafkaProducer<>(properties);

        //4.在main线程里使用send发送ProducerRecord
        for (int i = 0; i < 200; i++) {
            //添加回调
            kfkPro.send(new ProducerRecord<String, String>("third", "kafka1.8" + i), new Callback() {
                // 该方法在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        System.out.println("success->"+metadata.offset());
                    }else {
                        exception.printStackTrace();
                    }
                }
            });
            Thread.sleep(20);
        }
        //5.关闭资源
        kfkPro.close();

    }
}
