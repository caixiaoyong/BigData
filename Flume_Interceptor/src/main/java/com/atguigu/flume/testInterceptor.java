package com.atguigu.flume;

import com.alibaba.fastjson.JSON;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @author CZY
 * @date 2021/12/23 15:29
 * @description testInterceptor
 */
public class testInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = body.toString();



        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        return null;
    }

    @Override
    public void close() {

    }
}
