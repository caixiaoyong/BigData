package com.atguigu.Hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author CZY
 * @date 2021/11/8 19:13
 * @description HBaseUtilDML
 */
public class HBaseUtilDML {
    public static Connection connection=HBaseUtilDDL.connection;

    /**
     *
     * 插入数据
     *
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param rowKey       rowKey
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @param value        值
     * @throws IOException 连接错误
     */
    public static void putcell(String namespace,String tableName,String rowKey,String columnFamily,String columnName,String value) throws IOException {
        // 1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2.1创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));//将String类型转换成byte类型
        // 2.2添加put属性
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));

        // 2.插入数据，需要Put对象
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3.关闭table
        table.close();
    }

    /**
     * 读取数据
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param rowKey       rowKey
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @throws IOException 连接错误
     */
    public static void getcell(String namespace,String tableName,String rowKey,String columnFamily,String columnName) throws IOException {
        // 1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2.1创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 2.2添加get属性
        //get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        get.readVersions(3);

        // 2.获取数据,需要get对象
        Result result = null;
        try {
            result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(new String(CellUtil.cloneRow(cell))+ "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)) );
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    /**
     * 删除列族数据
     * @param namespace    命名空间名称
     * @param tableName    表格名称
     * @param rowKey       rowKey
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @throws IOException 连接错误
     */
    public static void deleteColumn(String namespace, String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        // 1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));     // 2.1创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 2.2添加删除信息
        // 2.2.1 删除最新一个版本
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        // 2.2.2删除一列所有版本
//        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        // 2.2.3 删除列族
//        delete.addFamily(Bytes.toBytes(columnFamily));

        // 2.删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    /**
     *扫描表格数据
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @param startRow 起始row
     * @param stopRow 结束row
     * @throws IOException 连接错误
     */
    public static void scanTable(String namespace, String tableName, String startRow,String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));     // 扫描表格
        Scan scan = new Scan();
        // 添加row限定
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        //scanner读取的是多行--二维数组
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(scan);

            for (Result result : scanner) {
                //一行row的数据
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println(new String(CellUtil.cloneRow(cell))+ "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)) + "\t");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    table.close();
    }

    public static void main(String[] args) throws IOException {
        // 测试putCell方法
        putcell("bigdata1", "student", "1001", "info", "age", "26");

        // 测试getCell方法
        getcell("bigdata1", "student", "1001", "info", "age");
        System.out.println("------=====-----");
        // 测试删除数据
        deleteColumn("bigdata1", "student", "1001", "info", "age");
        System.out.println("-------+++++-----");
        getcell("bigdata1", "student", "1001", "info", "age");

        System.out.println("----------------");
        // 测试scanTable
        scanTable("default","student","1001","1004");
    }




}
