package com.hqbhoho.bigdata.guava.concurrent.ratelimiter;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/11
 */
public class LeakyBucketRateLimiter {
    public static void main(String[] args) {
        RateLimiter rateLimiter = RateLimiter.create(2);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();

        System.out.println(LocalDateTime.now());

        new Thread(
                ()->{
                    while(true){
                        if(queue.size() < 10){
                            queue.add(atomicInteger.getAndIncrement());
                            System.out.println(atomicInteger.get()+" in queue,queue size: "+ queue.size());
                            try {
                                TimeUnit.MILLISECONDS.sleep(200);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }else {
                            System.out.println(LocalDateTime.now()+",queue is full...");
                            try {
                                TimeUnit.MILLISECONDS.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
        ).start();

        new Thread(
                ()->{
                    for(;;){
                        Stopwatch started = Stopwatch.createStarted();
                        double wait = rateLimiter.acquire();
                        Integer poll = queue.poll();
                        System.out.println(Thread.currentThread().getName() + "<===> process "
                                + poll + "<===> 耗时：" + started.stop() + "<===> 获取令牌耗时：" + wait);
                    }
                }
        ) .start();
        /*IntStream.range(0, 1).forEach(
                i -> new Thread(
                        () -> {
                            for(;;) {
                                Stopwatch started = Stopwatch.createStarted();
                                double wait = rateLimiter.acquire();
                                System.out.println(Thread.currentThread().getName() + "<===> process "
                                        + atomicInteger.getAndIncrement() + "<===> 耗时：" + started.stop() + "<===> 获取令牌耗时：" + wait);
                            }
                        }
                ).start()
        );*/
    }
}
