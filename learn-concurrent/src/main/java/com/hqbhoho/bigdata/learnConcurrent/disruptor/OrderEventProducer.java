package com.hqbhoho.bigdata.learnConcurrent.disruptor;

import com.lmax.disruptor.RingBuffer;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/23
 */
public class OrderEventProducer {
    private RingBuffer<OrderEvent> ringBuffer;

    public OrderEventProducer(RingBuffer<OrderEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void sendData(long i){
        long seq = ringBuffer.next();
        try {
            OrderEvent event = ringBuffer.get(seq);
            event.setValue(i);
        } finally {
            ringBuffer.publish(seq);
        }

    }
}
