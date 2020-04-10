package com.hqbhoho.bigdata.learnNio.multiplereactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * describe:
 * 处理IO请求
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/03/28
 */
public class SubReactor {
    private static final Logger LOG = LoggerFactory.getLogger(SubReactor.class);
    private Selector selector;
    private ExecutorService executorService;
    private LinkedBlockingQueue<SocketChannel> socketChannels;
    private Executor executor;
    private Thread thread;

    public SubReactor(ExecutorService executorService) {
        this.executorService = executorService;
        this.executor = new Executor() {
            @Override
            public void execute(Runnable runnable) {
                new Thread(runnable).start();
            }
        };
        this.socketChannels = new LinkedBlockingQueue<>();
    }

    private void init() {
        try {
            if (executorService == null) {
                this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            }
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addChannel(SocketChannel channel) {
        this.socketChannels.offer(channel);
    }


    public void addChannel0(SocketChannel channel) {
        try {
            channel.configureBlocking(false);
            channel.register(this.selector, SelectionKey.OP_READ | SelectionKey.OP_CONNECT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void wakeup() {
        this.selector.wakeup();
    }

    public void start() {
        executor.execute(()->{
            this.thread = Thread.currentThread();
            init();
            while (true) {
                try {
                    SocketChannel socketChannel = this.socketChannels.poll();
                    if(socketChannel != null){
                        addChannel0(socketChannel);
                    }
                    // selector 开始轮询事件
                    this.selector.select(500) ;
                    Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        SelectionKey key = keys.next();
//                        this.executorService.submit(new ServerRequestHandler(this.selector, key));
                        new ServerRequestHandler(this.selector, key).run();
                        keys.remove();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        });
    }
}