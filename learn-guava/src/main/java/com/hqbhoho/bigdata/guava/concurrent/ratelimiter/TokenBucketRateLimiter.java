package com.hqbhoho.bigdata.guava.concurrent.ratelimiter;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/11
 */
public class TokenBucketRateLimiter {

    public static void main(String[] args) {
        int phones_limit = 100;
        RateLimiter rateLimiter = RateLimiter.create(3);
        AtomicInteger orderNo = new AtomicInteger(0);
        Monitor monitor = new Monitor();

        Stopwatch started = Stopwatch.createStarted();
        CountDownLatch countDownLatch = new CountDownLatch(120);

        IntStream.range(0,120).forEach(
                i-> new Thread(
                        ()->{
                            while (true) {
                                if (rateLimiter.tryAcquire(3, TimeUnit.SECONDS)) {
                                    if (monitor.enterIf(monitor.newGuard(() -> orderNo.get() < phones_limit))) {
                                        try{
                                            int andIncrement = orderNo.getAndIncrement();
                                            System.out.println(Thread.currentThread().getName() + "<===> get phone orderNo: "+orderNo);
                                            countDownLatch.countDown();
                                            break;
                                        }finally {
                                            monitor.leave();
                                        }
                                    }else{
                                        System.out.println(Thread.currentThread().getName() + "<===> buy phone next time!");
                                        countDownLatch.countDown();
                                        break;
                                    }
                                }else {
                                    System.out.println(Thread.currentThread().getName() + "<===> get Token Failure...,retry after 1s. ");
                                    try {
                                        TimeUnit.SECONDS.sleep(1);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                ).start()
        );
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("本次抢购耗时共计："+started.stop());

    }
}
