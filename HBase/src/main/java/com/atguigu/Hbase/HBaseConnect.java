package com.atguigu.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 创建连接
 * @author CZY
 * @date 2021/11/8 11:17
 * @description HBaseConnect
 */
public class HBaseConnect {
    public static void main(String[] args) throws IOException {

        //1.获取配置类
        Configuration conf = HBaseConfiguration.create();
        //2.给配置类添加配置
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //3.获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //打印连接
        System.out.println(connection);
        //4.关闭连接
        connection.close();
    }
}
