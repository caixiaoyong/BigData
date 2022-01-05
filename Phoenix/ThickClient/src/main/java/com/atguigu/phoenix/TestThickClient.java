package com.atguigu.phoenix;

import java.sql.*;

/**
 * @author CZY
 * @date 2021/11/9 20:02
 * @description TestThickClient
 */
public class TestThickClient {
    public static void main(String[] args) throws SQLException {
        //获取url地址
        String url = "jdbc:phoenix:hadoop102:2181";
        //获取jdbc连接--连接驱动在依赖客户端5.0.0-HBase-2.0中给了
        Connection connection = DriverManager.getConnection(url);

        //编译sql
        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");

        //执行sql
        ResultSet resultSet = preparedStatement.executeQuery();

        //打印数据
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "-" + resultSet.getString(2) + "-" + resultSet.getString(3));
        }

        //关闭连接
        connection.close();
    }
}
