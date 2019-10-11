package com.hqbhoho.bigdata.guava.eventbus.customer.test;

import com.hqbhoho.bigdata.guava.eventbus.customer.Subscribe;

import java.util.concurrent.TimeUnit;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/10
 */
public class CustomerBusListener {

    @Subscribe
    public void test1(String event){
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread() + "process event : "+ event+" in CustomerBusListener test1");
    }

    @Subscribe()
    public void test2(String event){
        System.out.println(Thread.currentThread() + "process event : "+ event+" in CustomerBusListener test2");
    }

   /* @Subscribe
    public void test3(String event){
        throw new RuntimeException("程序报错了...");
    }*/

    @Subscribe
    public void test4(Integer event){
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread() + "process event : "+ event+" in CustomerBusListener test4");
    }
}
