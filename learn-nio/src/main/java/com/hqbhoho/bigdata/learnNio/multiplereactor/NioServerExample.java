package com.hqbhoho.bigdata.learnNio.multiplereactor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * describe:
 *
 * 多Reactor多工作线程 NIO  reactor模式
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/03/28
 */
public class NioServerExample {
    public static void main(String[] args) {
            ExecutorService workpool = Executors.newFixedThreadPool(16);
            workpool.submit(new MainReactor(19999,3,workpool));
    }
}
