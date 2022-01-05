package com.atguigu.flume;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author CZY
 * @date 2021/11/2 18:28
 * @description Mysink
 */
public class Mysink extends AbstractSink implements Configurable {

    private String prefix;
    private String suffix;

    //创建Logger对象
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    @Override
    public Status process() throws EventDeliveryException {
        //声明返回值状态信息
        Status status = null;

        // Start transaction
        //获取当前Sink绑定的Channel
        Channel ch = getChannel();
        //获取事务
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do
            //处理事件（打印）

            Event event = ch.take();

            LOG.info(prefix + new String(event.getBody()) + suffix);
            // Send the Event to the external repository.
            // storeSomeData(e);

            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();

            // Log exception, handle individual exceptions as needed

            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }

    /**
     * 读取配置文件中的文件信息
     * @param context
     */
    @Override
    public void configure(Context context) {
        //读取配置文件内容，有默认值
        prefix = context.getString("prefix", "hello");
        //读取配置文件内容，无默认值
        suffix = context.getString("suffix");
    }
}
