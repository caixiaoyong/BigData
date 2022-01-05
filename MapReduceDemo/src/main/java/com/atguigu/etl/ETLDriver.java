package com.atguigu.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 每行字段长度都大于11
 *
 * @author CZY
 * @date 2021/10/21 9:39
 * @description ETLDriver
 */
public class ETLDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        //2
        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);
        //3.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //4
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置reducetask个数为0
        job.setNumReduceTasks(0);
        //5
        FileInputFormat.setInputPaths(job, new Path("F:\\development\\hadoop_Test\\input\\etl.txt"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\development\\hadoop_Test\\out9"));
        //6
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
