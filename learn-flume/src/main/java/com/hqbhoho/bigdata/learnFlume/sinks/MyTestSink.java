package com.hqbhoho.bigdata.learnFlume.sinks;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * describe:
 * 自定义一个类似logger的sink,要求每个event的header中加入event在该sink中处理的时间戳。并加入可配置的日志前缀
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/04
 */
public class MyTestSink extends AbstractSink implements Configurable {

    private final static Logger LOG = LoggerFactory.getLogger(MyTestSink.class);
    //配置文件配置项
    private final String LOG_PREFIX="log.prefix";
    //日志前缀
    private String log_prefix;
    //headers
    Map<String, String> headers = new HashMap<>();

    /**
     * 加载配置文件配置项
     * @param context
     */
    @Override
    public void configure(Context context) {
        log_prefix=context.getString("log.prefix");
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        try {
            transaction.begin();
            event = channel.take();
            if (event != null) {
                //主要处理逻辑
                headers.put("timeStamp", String.valueOf(System.currentTimeMillis()));
                event.setHeaders(headers);
                LOG.info(log_prefix+">>>>>>>>>Headers:[{}],Body:[{}]", event.getHeaders(), new String(event.getBody()));
            } else {
                status = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Throwable ex) {
            transaction.rollback();
            throw new EventDeliveryException("occur some error.", ex);
        } finally {
            transaction.close();
        }
        return status;
    }
}
