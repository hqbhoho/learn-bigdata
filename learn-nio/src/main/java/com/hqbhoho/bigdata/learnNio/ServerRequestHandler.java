package com.hqbhoho.bigdata.learnNio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/15
 */
public class ServerRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ServerRequestHandler.class);

    private Selector selector;

    private ByteBuffer buffer = ByteBuffer.allocate(1024);

    public ServerRequestHandler(Selector selector) {
        this.selector = selector;
    }

    /**
     * 客户请求连接
     */
    public void handleAccept(SelectionKey key) {
        ServerSocketChannel channel = (ServerSocketChannel) key.channel();
        try {
            SocketChannel socketChannel = channel.accept();
            InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
            LOG.info("<========>客户端(ip:{},port:{}),发起了连接.....", remoteAddress.getHostName(), remoteAddress.getPort());
            // 建立socketChannel处理其他读写连接请求
            socketChannel.configureBlocking(false);
            socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_CONNECT );
            selector.wakeup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 客户端连接建立
     */
    public void handleConnect(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        try {
            InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
            LOG.info("<========>客户端(ip:{},port:{})上线了,已与服务器端建立好连接。", remoteAddress.getHostName(), remoteAddress.getPort());
            if (socketChannel.isConnectionPending()) socketChannel.finishConnect();
            selector.wakeup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读请求
     */
    public void handleRead(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        buffer.clear();
        try {
            socketChannel.read(buffer);
            InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
            LOG.info("<========>客户端(ip:{},port:{}),发送了请求,请求内容:{},请求到达时间:{}",
                    remoteAddress.getHostName(), remoteAddress.getPort(),
                    new String(buffer.array()), System.currentTimeMillis());
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 写请求
     */
    public void handleWrite(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        buffer.clear();
        byte[] response = ("server time: " + System.currentTimeMillis()+"\r\n").getBytes();
        buffer.put(response,0,response.length);
        buffer.flip();
        try {
            socketChannel.write(buffer);
            key.interestOps(key.interestOps()& ~SelectionKey.OP_WRITE);
            selector.wakeup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
