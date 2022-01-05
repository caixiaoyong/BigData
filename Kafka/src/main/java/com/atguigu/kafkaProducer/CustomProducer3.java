package com.atguigu.kafkaProducer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author CZY
 * @date 2021/11/3 14:45
 * @description CustomProducer
 */
public class CustomProducer3 {
    public static void main(String[] args) {
        //2.根据第一步中需要传进的Properties信息，直接创建一个
        Properties properties = new Properties();
        //3.给kafka配置对象添加配置对象
        // properties.put("bootstrap.servers","hadoop102:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        // key,value序列化
        properties.put("key.serializer", "org.apache.kafkaProducer.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafkaProducer.common.serialization.StringSerializer");

        //添加自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.atguigu.partition.MyPartitioner");

        //1.直接调用创建构建好KafkaProducer对象
        KafkaProducer<String, String> kfkPro = new KafkaProducer<>(properties);

        //4.在main线程里使用send发送ProducerRecord
        for (int i = 0; i < 10; i++) {
            if (i%2==0){
                kfkPro.send(new ProducerRecord<String, String>("first", "kafka0 -" + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null){
                            System.out.println(metadata.toString());
                            System.out.println(metadata.offset());
                        }else{
                            exception.printStackTrace();
                        }
                    }
                });
            }else{
                kfkPro.send(new ProducerRecord<String, String>("first", "kafka1 -" + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null){
                            System.out.println(metadata.toString());
                            System.out.println(metadata);
                        }else{
                            exception.printStackTrace();
                        }
                    }
                });
            }
        }
        //5.关闭资源
        kfkPro.close();

    }
}
