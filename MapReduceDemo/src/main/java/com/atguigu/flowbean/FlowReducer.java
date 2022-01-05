package com.atguigu.flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/17 22:51
 * @description FlowReducer
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
    //4.1 封装写出的IntWritable上传、下载及总流量
    private FlowBean outV=new FlowBean();
    /**
     *
     * @param key  电话号码phone
     * @param values phone对应的上行、下行数据 key ：values={[],[]}
     * 13568436656 {[2481,24681,],[1116,954,]}
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //13568436656 {[2481,24681,],[1116,954,]}
        //定义求和变量
        int totalUpFlow=0;
        int totalDownFlow=0;
        //1.遍历集合
        for (FlowBean value : values) {
            totalUpFlow+=value.getUpFlow();
            totalDownFlow+=value.getDownFlow();
        }
        //封装设置的值
        outV.setUpFlow(totalUpFlow);
        outV.setDownFlow(totalDownFlow);
        outV.setSumFlow();
        //写出
        context.write(key,outV);

    }
}
