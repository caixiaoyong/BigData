package com.atguigu.ComparablePartition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/19 19:20
 * @description CpMapper
 */
public class CpMapper  extends Mapper<LongWritable,Text,CpBean,Text>{
    private CpBean outk = new CpBean();
    private Text outV = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //转化字符串
        String line = value.toString();
        //切割
        String[] phones = line.split("\t");
        //封装
        outk.setUpFlow(Integer.parseInt(phones[1]));
        outk.setDownFlow(Integer.parseInt(phones[2]));
        outk.setSumFlow();
        outV.set(phones[0]);
        //写出
        context.write(outk,outV);
    }
}
