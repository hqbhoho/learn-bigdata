package com.hqbhoho.bigdata.learnConcurrent.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/23
 */
public class OrderEventFactory implements EventFactory<OrderEvent> {
    @Override
    public OrderEvent newInstance() {
        return new OrderEvent() ;
    }
}


