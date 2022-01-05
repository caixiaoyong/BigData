package com.atguigu.flowbean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/17 22:51
 * @description FlowMapper
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
   //4.1 封装写出的Text和IntWritable传入到Reduce
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.转化字符串
        String line = value.toString();
        //2.切割
        String[] phones = line.split("\t");
        //3.提取对应的数据
        String phone = phones[1];
        String upFlow = phones[phones.length-3];
        String downFlow = phones[phones.length-2];
        //4.2 封装
        outK.set(phone);
        outV.setUpFlow(Integer.parseInt(upFlow));//调用包装类将String-->Integer
        outV.setDownFlow(Integer.parseInt(downFlow));
        outV.setSumFlow();
        //5.写出
        context.write(outK,outV);
    }
}
