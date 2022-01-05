package com.atguigu.ComparablePartition;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 要求每个省份手机号输出的文件中按照总流量内部排序。
 * @author CZY
 * @date 2021/10/19 19:20
 * @description FlowDriver
 */
public class CpDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2 关联本Driver类
        job.setJarByClass(CpDriver.class);
        //3 关联Mapper和Reducer
        job.setMapperClass(CpMapper.class);
        job.setReducerClass(CpReducer.class);
        //4 设置Map端输出数据的KV类型
        job.setMapOutputKeyClass(CpBean.class);
        job.setMapOutputValueClass(Text.class);
        //5 设置程序最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CpBean.class);
        //8 指定自定义分区器
        job.setPartitionerClass(CpPartition.class);
        //9 同时指定相应数量的ReduceTask
        job.setNumReduceTasks(5);
        //6 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("F:\\development\\hadoop_Test\\input\\hello2.txt"));
        FileOutputFormat.setOutputPath(job,new Path("F:\\development\\hadoop_Test\\out5"));
        //7 提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
