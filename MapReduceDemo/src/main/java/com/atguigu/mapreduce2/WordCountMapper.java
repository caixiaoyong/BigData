package com.atguigu.mapreduce2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author C Y
 * @date 2021/10/16 14:20
 * @description WordCountMapper
 *
 * 1.先继承mapper类
 * 2.思考泛型写什么
 * KEYIN, VALUEIN,
 *      keyin    LongWritable  偏移量(读到哪了)
 *      valuein  Text          读进来的那一行数据
 *
 * KEYOUT, VALUEOUT
 *      keyout    Text         单词
 *      valueout  IntWritable  1
 * 3.补充逻辑
 */

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    /**
     * 在类里声明，但是在setup里进行赋值
     * outK伴随这类的加载创建一次
     * outV暂不统计个数，定死为1个
     */

    private Text outK;
    private IntWritable outV;

    /**
     * Called once at the beginning of the task.
     * 在map执行之前先执行一次setup
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new IntWritable(1);
    }

    /**
     *
     * @param key  偏移量
     * @param value 一行数据
     * @param context 上下文对象。由他将数据传给reducer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.转化字符串 atguigu atguigu
        String line = value.toString();
        //2.切割
        String[] words = line.split(" ");
        //3.遍历
        for (String word : words) {
            /**
             * 重写map方法中的context.write((KEYOUT) key, (VALUEOUT) value);
             * context.write里面要求输出key是Text类型，value是IntWritable类型
             * 因为没有这两类型，所以new这两类型，考虑代码优化，将属性放在最外面。
             */

            outK.set(word);
            context.write(outK,outV);
        }
    }
}
