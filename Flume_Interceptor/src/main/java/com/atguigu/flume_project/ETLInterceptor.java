package com.atguigu.flume_project;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.velocity.runtime.directive.Foreach;

import java.util.Iterator;
import java.util.List;

/**
 * 1. 实现系统提供的拦截器接口
 * 2. 实现拦截器的抽象方法
 * 3. 创建静态子类继承builder
 * @author CZY
 * @date 2021/11/13 11:13
 * @description ETLInterceptor
 */
public class ETLInterceptor implements Interceptor{
    @Override
    public void initialize() {

    }

    // 需求: 将event中的数据不是json的删除掉
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body);

        return JSONUtils.isJSONValidate(log) ?event:null;

    }

    @Override
    public List<Event> intercept(List<Event> events) {
        Iterator<Event> iterator = events.iterator();

        while (iterator.hasNext()) {
            Event next = iterator.next();
            while (intercept(next) == null) {
                iterator.remove();
            }
        }
        //foreach不能使用remove 删除为null的event
        /*for (Event event : events) {
            if (event==null){

            }
        }*/
        return events;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
