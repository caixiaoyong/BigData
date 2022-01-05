package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author C Y
 * @date 2021/10/15 10:16
 * @description HdfsClient
 * 1.百度
 * 2.所有客户端程序代码 套路一样：
 * 获取客户端对象
 * 进行操作
 * 关闭客户端对象
 * 3.创建文件系统对象 cy
 */
public class HdfsClient {
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        /**
         * 参数解读
         * 1.uri  hadoop102:8020
         * 2.conf
         * 3.user
         */
        //1.创建uri对象
        URI uri = new URI("hdfs://hadoop102:8020");

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(uri, configuration, "cy");
        //2.创建目录
        fs.mkdirs(new Path("/shuihu/108haohan"));
        //3.关闭资源
        fs.close();
    }
 }

