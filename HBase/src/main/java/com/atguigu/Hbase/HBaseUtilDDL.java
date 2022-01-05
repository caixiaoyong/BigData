package com.atguigu.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 官方标准使用--单例模式
 * @author CZY
 * @date 2021/11/8 11:43
 * @description HBaseUtilDDL
 */
public class HBaseUtilDDL {
    public static Connection connection;
    //0.使用静态代码块的饿汉单例--创建连接
    static {

        //1.获取配置类
        Configuration conf = HBaseConfiguration.create();
        //2.给配置类添加配置
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //3.获取连接
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 避免空指针异常
    public static void closeConnection() throws IOException {
        if (connection != null){
            connection.close();
        }
    }

    /**
     * 创建命名空间
     * @param namespace 命名空间的名称
     * @throws IOException 连接错误
     */
    public static void createNamespace(String namespace) throws IOException {
        //获取admin
        Admin admin = connection.getAdmin();
        //调用方法创建命名空间--建造者模式
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);
        builder.addConfiguration("user","cy");

        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println("命名空间已经存在");
        }
        //关闭admin
        admin.close();
    }

    /**
     *
     * 判断表格是否存在
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @return true存在 false不存在
     * @throws IOException 连接出错
     */
    public static boolean isTableExists(String namespace,String tableName) throws IOException {
        //获取admin
        Admin admin = connection.getAdmin();

        boolean tmp =false;
        //判断表格是否存在
        try {
           tmp = admin.tableExists(TableName.valueOf(namespace,tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //关闭admin
        admin.close();
        return tmp;
    }

    /**
     * 创建表格
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @param columnFamilies 列族名称 至少一个
     * @throws IOException 连接报错
     */
    public static void createTable(String namespace,String tableName,String... columnFamilies) throws IOException {
        //1.判断表格是否存在
        if(isTableExists(namespace,tableName)){
            System.out.println("表格已存在");
            return;
        }
        //2.如果填写的列族为0个
        if (columnFamilies.length == 0){
            System.out.println("至少要有一个列族");
            return;
        }
        //3.获取admin
        Admin admin = connection.getAdmin();

        //4.1调用方法创建表格,获取表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        //4.2添加列族
        for (String columnFamily : columnFamilies) {
            // 获取列族描述的建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));

            // 给单独的一个列族修改参数
            columnFamilyDescriptorBuilder.setMaxVersions(5);

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        //4.创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        //5.关闭admin
        admin.close();
    }

    /**
     *
     * 修改表格一个列族的版本信息
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @param columnFamily 列族名称
     * @param version 修改的版本号
     * @throws IOException 连接出错
     */
    public static void alterTable(String namespace,String tableName,String columnFamily,int version) throws IOException {
        //1. 获取admin
        Admin admin = connection.getAdmin();
        // 判断表格是否存在
        if (!isTableExists(namespace,tableName)){
            System.out.println("表格不存在");
            return;
        }
        //2.1 修改表格信息  需要使用原先的表格描述进行修改
        TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));
        //获取表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);

        //2.2 获取原先的列族描述
        ColumnFamilyDescriptor columnFamilyDescriptor = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));
        //获取列族描述的建造者
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyDescriptor);

        columnFamilyDescriptorBuilder.setMaxVersions(version);

        tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());

        //2.修改表
        try {
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        //3.关闭admin
        admin.close();
    }

    /**
     *
     * 删除表格
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @throws IOException 连接错误
     */
    public static void deleteTable(String namespace,String tableName) throws IOException {
        //1. 获取admin
        Admin admin = connection.getAdmin();
        // 判断表格是否存在
        if (!isTableExists(namespace,tableName)){
            System.out.println("表格不存在");
            return;
        }
        //删除表格前闲进行不可用标记
        try {
            TableName tableName1 = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableName1);//.var选择两个都使用新对象
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //调用方法删除表格

        //3.关闭admin
        admin.close();
    }

    public static void main(String[] args) throws IOException {
        //4.使用hbase的连接
        System.out.println(connection);

        // 测试创建命名空间
        createNamespace("bigdata1");

        // 测试表格是否存在
        System.out.println(isTableExists("bigdata1", "student"));

        // 测试创建表格
        createTable("bigdata1","student","info","msg");

        //测试修改表格
        alterTable("bigdata1","student","info",3);
        System.out.println("创建成功");

        //删除表格
        deleteTable("bigdata1","student3");
        System.out.println("删除成功");

        System.out.println("最后输出");
        // 所有代码的最后
        closeConnection();
    }
}
