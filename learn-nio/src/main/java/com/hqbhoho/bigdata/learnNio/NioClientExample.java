package com.hqbhoho.bigdata.learnNio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * describe:
 * nio client example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/17
 */
public class NioClientExample {
    public static void main(String[] args) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress("localhost", 8888));
            Selector selector = Selector.open();
            socketChannel.register(selector, SelectionKey.OP_CONNECT);
            while (true) {
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                ClientRequestHandler requestHandler = new ClientRequestHandler(selector);
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    // 根据Key，处理客户端不同的请求。
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
        }
    }
}
