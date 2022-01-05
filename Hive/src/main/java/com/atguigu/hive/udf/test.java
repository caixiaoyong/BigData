package com.atguigu.hive.udf;

import java.util.Arrays;
import java.util.List;

/**
 * @author CZY
 * @date 2021/10/29 16:55
 * @description test
 */
public class test {
    public static void main(String[] args) {
        List<String> list = Arrays.asList("Hello", "World!"); list.stream().forEach(System.out::println);
    }
}
