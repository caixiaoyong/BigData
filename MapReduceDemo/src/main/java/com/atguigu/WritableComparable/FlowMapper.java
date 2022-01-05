package com.atguigu.WritableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/17 22:51
 * @description FlowMapper
 */
public class FlowMapper extends Mapper<LongWritable,Text,FlowBean,Text>{
   //3.1 封装写出的Text和IntWritable传入到Reduce
    private FlowBean outk = new FlowBean();
    private Text outv = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行数据
        String line = value.toString();

        //2.按照“\t”，切割数据
        String[] phones = line.split("\t");

        //3.封装outk，outV
        outk.setUpFlow(Integer.parseInt(phones[1]));
        outk.setDownFlow(Integer.parseInt(phones[2]));
        outk.setSumFlow();
        outv.set(phones[0]);

        //4.写出context.write里面要求输出key是FlowBean类型--排序后的流量，value是Text类型
        context.write(outk,outv);


    }
}
