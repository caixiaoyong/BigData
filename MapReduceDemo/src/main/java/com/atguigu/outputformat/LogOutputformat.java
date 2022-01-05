package com.atguigu.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 输出泛型来自Reducer的输出
 * @author CZY
 * @date 2021/10/19 21:01
 * @description LogOutputformat
 */
public class LogOutputformat extends FileOutputFormat<Text,NullWritable>{
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        //因为返回值是RecordWriter，我们没有，所以创建一个自定义的RecorWriter返回
        //可以通过job.getConfiguration()获取到Configuration
        LogRecordWriter logRecordWriter = new LogRecordWriter(job);
        return logRecordWriter;
    }
}
