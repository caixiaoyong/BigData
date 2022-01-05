package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Arrays;


/**
 * @author C Y
 * @date 2021/10/15 13:55
 * @description Hdfs_Client
 */
public class Hdfs_Client {

    private FileSystem fs;
    private Configuration conf;

    @Before //在test方法运行之前 执行一次
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:8020");
        conf = new Configuration();//ctrl+alt+f 将变量升级为全局
        fs = FileSystem.get(uri, conf,"cy");
    }
    @After //在test之后执行一次
    public void close() throws IOException {
        fs.close();
    }
    @Test
    public void mkdir() throws IOException {
        fs.mkdirs(new Path("/SHUIHU"));
    }

    @Test
    public void put() throws IOException {
        /**
         * 参数解读
         * 1.boolean delSrc 是否删除源文件  windows
         * 2.boolean overwrite 是否覆盖目标文件 hdfs
         * 3.Path src 源文件路径 windows
         * 4. Path dst 目标路径 hdfs
         */
        fs.copyFromLocalFile(false,true,new Path("F:\\development\\test.txt"),
                new Path("/shuihu"));
    }

    @Test
    public void get() throws IOException {
        //1.boolean delSrc 是否删除源文件 hdfs
        //2.Path src 源文件路径
        //3. Path dst 目标文件路径
        //4.boolean useRawLocalFileSystem //是否校验  true不检验  false 校验 --生成crc文件
        fs.copyToLocalFile(true,new Path("/shuihu/test.txt"),
                new Path("F:\\development"),true);
    }

    @Test
    public void mv() throws IOException {
        fs.rename(new Path("/Java/java1"),new Path("/Java/test"));
    }

    @Test
    public void rm() throws IOException {
        //1.Path src 源文件路径
        //2.recursive 递归
        fs.delete(new Path("/SHUIHU"),true);
    }

    @Test
    public void ls() throws IOException {
        RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("/"), true);
        while (list.hasNext()) {
            LocatedFileStatus fi = list.next();
            System.out.println("==============="+fi.getPath()+"===============");
            System.out.println(fi.getPermission());
            System.out.println(fi.getOwner());
            System.out.println(fi.getGroup());
            System.out.println(fi.getLen());
            SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println(sd.format(fi.getModificationTime()));
            System.out.println(fi.getReplication());
            System.out.println(fi.getBlockSize());
            System.out.println(fi.getPath().getName());
            BlockLocation[] blockLocations = fi.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void fileOrDir() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()){
                System.out.println("文件: "+fileStatus.getPath());
            }else {
                System.out.println("目录： "+fileStatus.getPath());
            }
        }
    }

    public void fileOrDir(Path path) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件："+fileStatus.getPath());
            }else {
                System.out.println("目录:"+fileStatus.getPath());
                fileOrDir(fileStatus.getPath());
            }
        }
    }

    @Test
    public void testfile() throws IOException {
        fileOrDir(new Path("/"));
    }
    
}
