package com.hqbhoho.bigdata.learnConcurrent.thread;

import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/07
 * @see Thread  ThreadLocalMap threadLocals = null;
 * @see ThreadLocal.ThreadLocalMap  this.table = new ThreadLocal.ThreadLocalMap.Entry[16];
 * 每个线程内部维护维护一个数组，当线程内部调用threadLoacl.set()方法
 * 实际是将 Entry(ThreadLocal,value) 放入之前创建的数组中。
 * 数组角标的确认是根据ThreadLocal内部维护唯一标识计算出来的。
 * @see ThreadLocal  threadLocalHashCode
 * ThreadLocal.ThreadLocalMap.Entry[] var3 = this.table;
 * int var5 = var1.threadLocalHashCode & var4 - 1;
 * var3[var5] = new ThreadLocal.ThreadLocalMap.Entry(var1, var2);
 */
public class ThreadLocalDemo {
    public static void main(String[] args) {
        ThreadLocal<String> threadLocal0 = new ThreadLocal<>();
        ThreadLocal<Integer> threadLocal1 = new ThreadLocal<>();
        IntStream.rangeClosed(0, 10).forEach(i -> {
            new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            new ReentrantLock().lock();
                            new ReentrantLock().unlock();
                            threadLocal0.set(i + "");
                            threadLocal1.set(i);
                            Thread thread = Thread.currentThread();
                        }
                    }
            ).start();


        });


    }
}
