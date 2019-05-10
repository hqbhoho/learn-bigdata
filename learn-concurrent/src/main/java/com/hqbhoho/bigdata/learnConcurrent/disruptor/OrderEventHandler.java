package com.hqbhoho.bigdata.learnConcurrent.disruptor;

import com.lmax.disruptor.EventHandler;

import java.util.Optional;

/**
 * describe:
 * consumer and process event
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/23
 */
public class OrderEventHandler implements EventHandler<OrderEvent> {
    @Override
    public void onEvent(OrderEvent orderEvent, long l, boolean b) throws Exception {
        Optional.ofNullable(orderEvent.getValue()).ifPresent(System.out::println);
        Optional.ofNullable(l).ifPresent(System.out::println);
        Optional.ofNullable(b).ifPresent(System.out::println);
    }
}
