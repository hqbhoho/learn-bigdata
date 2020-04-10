package com.hqbhoho.bigdata.learnNio.multiplereactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

/**
 * describe:
 * 处理accept请求
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/03/28
 */
public class MainReactor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MainReactor.class);

    private int port;
    private SubReactor[] subReactors;
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;

    public MainReactor(int port, int subReactorNum, ExecutorService executorService) {
        this.port = port;
        this.subReactors = new SubReactor[subReactorNum];
        for (int i = 0; i < subReactorNum; i++) {
            subReactors[i] = new SubReactor(executorService);
        }
    }

    private void init() {
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = Selector.open();
            this.serverSocketChannel.socket().bind(new InetSocketAddress(this.port));
            this.serverSocketChannel.configureBlocking(false);
            // serverSocketChannel 只关心 accept事件，即监听客户端发起连接请求事件
            this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            Arrays.asList(subReactors).stream().forEach(recator-> recator.start());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        init();
        int index = 0;
        while (true) {
            // selector 开始轮询事件
            try {

                this.selector.select(500);
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    // 根据Key，处理客户端不同的请求。这里selector 只处理accept事件
                    if (key.isAcceptable()) {
                        ServerSocketChannel channel = (ServerSocketChannel) key.channel();
                        SocketChannel socketChannel = channel.accept();
                        InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
                        LOG.info("MainReactor处理线程：" + Thread.currentThread().getId() +
                                        "<========>客户端(ip:{},port:{}),发起了连接.....",
                                remoteAddress.getHostName(), remoteAddress.getPort());
                        // 建立socketChannel处理其他读写连接请求
                        int num = index++ % subReactors.length;
                        SubReactor subReactor = subReactors[num];
                        LOG.info("MainReactor处理线程："+Thread.currentThread().getId() +"分发IO请求,"+num+" 号subReactor获取执行权。");
                        subReactor.addChannel(socketChannel);
                        subReactor.wakeup();
                    }
                    keys.remove();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}