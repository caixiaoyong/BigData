package com.atguigu.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author C Y
 * @date 2021/10/16 14:20
 * @description WordCountReducer
 * 1.继承reduce
 * 2.思考泛型
 * KEYIN,VALUEIN,  一定是mapper输出进来的
 * KEYOUT,VALUEOUT
 *      keyout 单词 Text
 *      valueout 总次数 IntWritable
 * 3.补充逻辑
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    private IntWritable outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new IntWritable();
    }

    /**
     * @param key     单词
     * @param values  当前单词的所有值个数--atguigu ：values=2 [1,1]
     * @param context 上下文对象  将数据写出文件中
     * @throws IOException
     * @throws InterruptedException
     */
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
