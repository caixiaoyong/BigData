package com.atguigu.flume_project;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @author CZY
 * @date 2021/11/13 16:00
 * @description TimeStampInterceptor
 */
public class TimeStampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 需求: 将数据中的ts时间  保存到头信息中   供hdfs sink使用  放入对应的文件夹
        String log = new String(event.getBody());

        //将log字符串转化成JSON Object类型的，可以使用getString方法获取json里面的string
        JSONObject jsonObject = JSONObject.parseObject(log);
        //拿出日志里面的ts时间戳、再将其放在头信息里面
        String ts = jsonObject.getString("ts");
        Map<String, String> headers = event.getHeaders();
        headers.put("timestamp",ts);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new TimeStampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
