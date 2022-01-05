package com.atguigu.WritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 由Mapper传进来的是<FlowBean,Text>--<排序后的总流量，电话号码>
 *     再由Reducder以<Text,FlowBean>--<电话号码,排序后的总流量>传出去
 * @author CZY
 * @date 2021/10/17 22:51
 * @description FlowReducer
 */
public class FlowReducer extends Reducer<FlowBean,Text,Text,FlowBean>{
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //为了防止总流量相同导致缺少数据 迭代输出
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
