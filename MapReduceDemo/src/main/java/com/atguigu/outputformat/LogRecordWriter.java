package com.atguigu.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/19 21:01
 * @description LogRecordWriter
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private  FSDataOutputStream atguigu;
    private  FSDataOutputStream others;

    public LogRecordWriter(TaskAttemptContext job){
        try {
            //构造器内使用try、catch处理异常
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            atguigu = fileSystem.create(new Path("F:\\development\\hadoop_Test\\out6_log\\atguigu.txt"));
            others = fileSystem.create(new Path("F:\\development\\hadoop_Test\\out6_log\\other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 往外写的逻辑
     * 将reduce处理的结果一行一行的往外写
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //转化字符串
        String log = key.toString();
        //根据一行的log数据是否包含atguigu,判断两条输出流输出的内容
        if (log.contains("atguigu")){
            atguigu.writeBytes(log+"\n");
        }else {
            others.writeBytes(log+"\n");
        }
    }

    /**
     * 关流
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(atguigu,others);
    }
}
