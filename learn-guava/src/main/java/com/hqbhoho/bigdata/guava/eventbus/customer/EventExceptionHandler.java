package com.hqbhoho.bigdata.guava.eventbus.customer;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
@FunctionalInterface
public interface EventExceptionHandler {
    public void handle(Exception e, EventContext context);
}
