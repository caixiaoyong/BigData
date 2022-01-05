package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @author CZY
 * @date 2021/11/4 11:24
 * @description MyInterceptor
 */
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        if (body[0]<='z'&&body[0]>'a'||body[0]<='Z'&&body[0]>='A'){
            event.getHeaders().put("type","letter");
        }else if(body[0]<=9&&body[0]>=1){
            event.getHeaders().put("type","number");
        }
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
    public static class myBuild implements Builder{

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
