package com.atguigu.flowbean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/17 22:50
 * @description FlowDriver
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建配置对象并获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.绑定driver
        job.setJarByClass(FlowDriver.class);
        //3.绑定mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4.设置mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.设置最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //6.设置输入路径
        FileInputFormat.setInputPaths(job,new Path("F:\\development\\hadoop_Test\\input\\hello1.txt"));
        //7.设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("F:\\development\\hadoop_Test\\out3"));
        //8.提交运行
        //job.waitForCompletion(true);//为true监控并打印更多job
        boolean b = job.waitForCompletion(true);//为true监控并打印更多job
        System.exit(b?0:1);
    }
}
