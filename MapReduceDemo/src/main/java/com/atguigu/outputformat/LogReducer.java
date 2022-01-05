package com.atguigu.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/19 21:00
 * @description LogReducer
 */
public class LogReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //防止相同的数据，迭代写出
        for (NullWritable value : values) {
            context.write(key,value);
        }
    }
}
