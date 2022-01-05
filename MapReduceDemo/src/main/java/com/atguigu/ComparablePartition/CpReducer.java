package com.atguigu.ComparablePartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/19 19:21
 * @description CpReducer
 */
public class CpReducer extends Reducer<CpBean,Text,Text,CpBean> {
    @Override
    protected void reduce(CpBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //防止总流量相同导致缺少数据，所以采用迭代每个都输出 ，最后再反向写出
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
