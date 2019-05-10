package com.hqbhoho.bigdata.learnConcurrent.disruptor;

/**
 * describe:
 * disruptor  event
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/23
 */
public class OrderEvent {
    private Long value;

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
