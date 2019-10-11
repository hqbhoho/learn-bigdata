package com.hqbhoho.bigdata.guava.concurrent;

import com.google.common.util.concurrent.Monitor;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

/**
 * describe:
 * <p>
 * Concurrent Operator
 * synchronized
 * lock
 * guava monitor
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/11
 */
public class MonitorExample {
    public static void main(String[] args) {
        /*SynchronizedQueue queue = new SynchronizedQueue(10);*/
       /* LockedQueue queue = new LockedQueue(10);*/
        MonitorQueue queue = new MonitorQueue(10);
        AtomicInteger num = new AtomicInteger(0);
        IntStream.rangeClosed(0, 5).forEach(
                i -> new Thread(() -> {
                    for (; ; ) {
                        int andIncrement = num.getAndIncrement();
                        queue.produce(andIncrement);
                        System.out.println(Thread.currentThread() + "<---> produce: " + andIncrement);
                        try {
                            TimeUnit.MILLISECONDS.sleep(5);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }).start()
        );

        IntStream.rangeClosed(0, 2).forEach(
                i -> new Thread(
                        () -> {
                            for (; ; ) {
                                int consume = queue.consume();
                                System.out.println(Thread.currentThread() + "<---> consume: " + consume);
                                try {
                                    TimeUnit.MILLISECONDS.sleep(5);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                ).start()
        );

    }

    /**
     * thread safe class with monitor
     */
    static class MonitorQueue {
        private Queue<Integer> queue = new LinkedList<>();
        private int limit;
        private Monitor monitor = new Monitor();
        private Monitor.Guard notEmptyGuard = monitor.newGuard(() -> !this.queue.isEmpty());
        private Monitor.Guard notFullGuard = monitor.newGuard(() -> this.queue.size() < this.limit);


        public MonitorQueue(int limit) {
            this.limit = limit;
        }

        public void produce(int value) {
            try {
                monitor.enterWhen(notFullGuard);
                queue.add(value);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                monitor.leave();
            }
        }

        public int consume() {
            Integer res = null;
            try {
                monitor.enterWhen(notEmptyGuard);
                res = queue.poll();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                monitor.leave();
            }
            return res;
        }
    }

    /**
     * thread safe class with synchronized
     */
    static class SynchronizedQueue {
        private Queue<Integer> queue = new LinkedList<>();
        private int limit;

        public SynchronizedQueue(int limit) {
            this.limit = limit;
        }

        public void produce(int value) {
            synchronized (queue) {
                while (queue.size() >= this.limit) {
                    try {
                        queue.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                queue.add(value);
                queue.notifyAll();
            }
        }

        public int consume() {
            synchronized (queue) {
                while (queue.isEmpty()) {
                    try {
                        queue.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Integer res = queue.poll();
                queue.notifyAll();
                return res;
            }
        }
    }

    /**
     * thread safe class with synchronized
     */
    static class LockedQueue {

        private Queue<Integer> queue = new LinkedList<>();
        private int limit;

        private ReentrantLock lock = new ReentrantLock();
        private Condition emptyCondition = lock.newCondition();
        private Condition fullCondition = lock.newCondition();

        public LockedQueue(int limit) {
            this.limit = limit;
        }

        public void produce(int value) {
            try {
                lock.lock();
                while (queue.size() >= this.limit) {
                    try {
                        fullCondition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                queue.add(value);
                emptyCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }

        public int consume() {
            try {
                lock.lock();
                while (queue.isEmpty()) {
                    try {
                        emptyCondition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                Integer res = queue.poll();
                fullCondition.signalAll();
                return res;
            } finally {
                lock.unlock();
            }
        }
    }
}

