package com.hqbhoho.bigdata.learnNio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;

/**
 * describe:
 * NIO server example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/15
 */
public class NioServerExample {
    public static void main(String[] args) {
        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            Selector selector = Selector.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(8888));
            serverSocketChannel.configureBlocking(false);

            /*SelectorProvider provider = SelectorProvider.provider();
            Selector selector = provider.openSelector();
            ServerSocketChannel serverSocketChannel = provider.openServerSocketChannel();*/

            // serverSocketChannel 只关心 accept事件，即监听客户端发起连接请求事件
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (true) {
                // selector 开始轮询事件
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                ServerRequestHandler requestHandler = new ServerRequestHandler(selector);
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    // 根据Key，处理客户端不同的请求。
                    if (key.isAcceptable()) {
                        requestHandler.handleAccept(key);
                    }
                    if (key.isConnectable()) {
                        requestHandler.handleConnect(key);
                    }
                    if (key.isReadable()) {
                        requestHandler.handleRead(key);
                    }
                    if (key.isWritable()) {
                        requestHandler.handleWrite(key);
                    }
                    keys.remove();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }

    }
}
