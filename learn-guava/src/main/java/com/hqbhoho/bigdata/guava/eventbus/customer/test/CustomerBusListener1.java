package com.hqbhoho.bigdata.guava.eventbus.customer.test;

import com.hqbhoho.bigdata.guava.eventbus.customer.Subscribe;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
public class CustomerBusListener1 {
    @Subscribe
    public void test6(String event) {
        System.out.println(Thread.currentThread() + "process event : " + event + " in CustomerBusListener1 test6");
    }
}
