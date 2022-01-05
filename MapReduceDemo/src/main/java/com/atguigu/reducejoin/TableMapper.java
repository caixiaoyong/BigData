package com.atguigu.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/20 10:27
 * @description TableMapper
 */
public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean>{

    private String filename;
    private Text outk = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //2.获取切片信息、强转成FileSplit--InputSplit子类
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        //3.获取切片的文件名称，因为要在map中调用，所以提升为全局变量
        filename = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();

        //4.根据文件名判断是哪个表
        if (filename.contains("order")){//订单表处理
            String[] order = line.split("\t");
            //5.封装--以pid为key
            outk.set(order[1]);
            outV.setId(order[0]);
            outV.setPid(order[1]);
            outV.setAmount(Integer.parseInt(order[2]));
            outV.setPname("");//订单表没有，设置为空
            outV.setFlag("order");
        }else {//商品表处理
            String[] goods = line.split("\t");
            //封装
            outk.set(goods[0]);
            outV.setId("");//商品表没有id，设置为空
            outV.setPid(goods[0]);
            outV.setAmount(0);//商品表没有amount，设置为空
            outV.setPname(goods[1]);
            outV.setFlag("goods");
        }
        context.write(outk,outV);
    }
}
