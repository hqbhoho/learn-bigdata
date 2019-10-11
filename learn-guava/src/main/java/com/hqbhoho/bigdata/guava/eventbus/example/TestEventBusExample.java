package com.hqbhoho.bigdata.guava.eventbus.example;

import com.google.common.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/09
 */
public class TestEventBusExample {

    private final static Logger LOGGER = LoggerFactory.getLogger(TestEventBusExample.class);

    public static void main(String[] args) {

        EventBus eventBus = new EventBus((cause, context) -> {
            /**
             *
             * 自定义ExceptionHandler，Context中携带了上下文的详细信息
             */
            Object event = context.getEvent();
            EventBus eventBus1 = context.getEventBus();
            Object subscriber = context.getSubscriber();
            Method subscriberMethod = context.getSubscriberMethod();
            LOGGER.info("subscriber: " + subscriber + ",subscriberMethod: " + subscriberMethod + "" +
                    " process event [" + event + "] which from eventBus [" + eventBus1 + "] occur Exception!");
        });
        eventBus.register(new TestEventListener2());
//        eventBus.post("hahaha");
        eventBus.post(12345);
    }
}
