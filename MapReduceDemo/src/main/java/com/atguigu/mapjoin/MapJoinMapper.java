package com.atguigu.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author CZY
 * @date 2021/10/20 20:57
 * @description MapJoinMapper
 */
public class MapJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    private Text outK=new Text();
    private HashMap<String, String> pdMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //0.创建一个数据结构hashmap来装商品的结果集,放在全局
        pdMap = new HashMap<>();
        //1.获取缓存
        URI[] cacheFiles = context.getCacheFiles();
        //因为getCacheFiles()可以缓存多张表，所以我们这边需要获取缓存的第一张表
        Path path = new Path(cacheFiles[0]);

        //2.获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //3.将字节流转换成缓存字符流，使其可以读取一行
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        //4.逐行读取，按行处理
        String line;
        while((line=reader.readLine())!=null){
            //切割一行
            //01	小米
            String[] pdInfo = line.split("\t");
            pdMap.put(pdInfo[0],pdInfo[1]);
        }
        //关流
        IOUtils.closeStreams(reader,fis);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取大表数据
        //1001	01	1

        String line = value.toString();
        String[] orderInfo = line.split("\t");
        //通过order 里面pid 获取map中的pname
        String pname = pdMap.get(orderInfo[1]);
        //写出
        outK.set(orderInfo[0]+"\t"+pname+"\t"+orderInfo[2]);
        context.write(outK,NullWritable.get());
    }
}
