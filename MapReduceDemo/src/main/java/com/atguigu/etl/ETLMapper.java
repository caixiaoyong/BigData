package com.atguigu.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 在Map阶段对输入的数据根据规则进行过滤清洗
 * @author CZY
 * @date 2021/10/21 9:39
 * @description ETLMapper
 */
public class ETLMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取第一行数据
        String line = value.toString();
        //切割
        String[] fields = line.split(" ");
        if (fields.length>11){
            context.write(value,NullWritable.get());
        }

    }
}
