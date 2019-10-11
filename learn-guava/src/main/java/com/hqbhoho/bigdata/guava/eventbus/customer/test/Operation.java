package com.hqbhoho.bigdata.guava.eventbus.customer.test;

import com.hqbhoho.bigdata.guava.eventbus.customer.EventBus;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
public class Operation {
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        EventBus eventbus = EventBus.builder()
                .busName("aaa")
                .executor(executorService)
                .eventExceptionHandler((cause,context)->{
                    System.out.println(cause.getMessage() +",detail: " +Thread.currentThread() + "process event : "+ context.getEvent()
                            +" which from "+context.getBusName()+"---"+context.getTopic() +" in "
                            +context.getSubscriber());
                })
                .build();
        CustomerBusListener customerBusListener = new CustomerBusListener();
        eventbus.register(customerBusListener);
        eventbus.post("test1-message");
        eventbus.post(123);
        eventbus.unregister(customerBusListener);
        eventbus.post("test2-message");
        eventbus.close();
    }
}
