package com.atguigu.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 在Mapper输出之后 在Reducer输入之前
 * @author CZY
 * @date 2021/10/19 20:33
 * @description WordCombiner
 */
public class WordCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
    private IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //atguigu [1,1]
        int sum = 0;
        //1.遍历集合
        for (IntWritable value : values) {
            sum += value.get();
        }
        //设置值
        outV.set(sum);
        //写出
        context.write(key,outV);

    }
}
