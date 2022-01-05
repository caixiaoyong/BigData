package com.atguigu.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author CZY
 * @date 2021/11/3 16:09
 * @description MyPartitioner
 */
public class MyPartitioner implements Partitioner{
    /**
     * 返回信息对应的分区
     * @param topic 主题
     * @param key   消息的key
     * @param keyBytes  消息的key序列化后的字节数组
     * @param value     消息的value
     * @param valueBytes    消息的value序列化后的字节数组
     * @param cluster       集群元数据可以查看分区信息
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取消息
        String msgValue = value.toString();
        // 创建partition
        int partition;
        // 判断消息是否包含atguigu
        if (msgValue.contains("atguigu")){
            partition = 0;
        }else {
            partition =1;
        }
        // 返回分区号
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
