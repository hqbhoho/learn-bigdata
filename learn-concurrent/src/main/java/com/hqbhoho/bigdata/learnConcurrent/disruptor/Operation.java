package com.hqbhoho.bigdata.learnConcurrent.disruptor;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.IntStream;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/23
 */
public class Operation {
    public static void main(String[] args) {

        OrderEventFactory factory = new OrderEventFactory();
        int ringBufferSize = 1024 * 1024;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        Disruptor disruptor = new Disruptor(factory, ringBufferSize, threadFactory, ProducerType.SINGLE, new BlockingWaitStrategy());

        disruptor.handleEventsWith(new OrderEventHandler());
        disruptor.start();
        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();

        OrderEventProducer producer = new OrderEventProducer(ringBuffer);

        IntStream.rangeClosed(0, 99).forEach(producer::sendData);

        disruptor.shutdown();

    }
}
