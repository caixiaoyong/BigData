package com.atguigu.flume_project;

import com.alibaba.fastjson.JSON;

/**
 * @author CZY
 * @date 2021/11/13 11:20
 * @description JSONUtils
 */
public class JSONUtils {
    public static boolean isJSONValidate(String log){
        try {
            //解析字符串 是不是json格式
            JSON.parse(log);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
