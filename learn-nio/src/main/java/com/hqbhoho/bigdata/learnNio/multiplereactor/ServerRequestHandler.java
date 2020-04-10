package com.hqbhoho.bigdata.learnNio.multiplereactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/15
 */
public class ServerRequestHandler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerRequestHandler.class);

    private Selector selector;
    private SelectionKey key;

//    private ByteBuffer buffer = ByteBuffer.allocate(1024);

    public ServerRequestHandler(Selector selector, SelectionKey key) {
        this.selector = selector;
        this.key = key;
    }

    @Override
    public void run() {

        if (this.key.isConnectable()) {
            handleConnect();
        }
        if (this.key.isReadable()) {
            handleRead();
        }
        if (this.key.isWritable()) {
            handleWrite();
        }


    }

    /**
     * 客户端连接建立
     */
    public void handleConnect() {
        SocketChannel socketChannel = (SocketChannel) this.key.channel();
        try {
            InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
            LOG.info("workpool处理线程：" + Thread.currentThread().getId() + "<========>客户端(ip:{},port:{})上线了,已与服务器端建立好连接。", remoteAddress.getHostName(), remoteAddress.getPort());
            if (socketChannel.isConnectionPending()) socketChannel.finishConnect();
            selector.wakeup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读请求
     */
    public void handleRead() {
        SocketChannel socketChannel = (SocketChannel) this.key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try {
            socketChannel.read(buffer);
            if(buffer.hasRemaining()){
                InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
                LOG.info("workpool处理线程：" + Thread.currentThread().getId() + "<========>客户端(ip:{},port:{}),发送了请求,请求内容:{},请求到达时间:{}",
                        remoteAddress.getHostName(), remoteAddress.getPort(),
                        new String(buffer.array()), System.currentTimeMillis());
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 写请求
     */
    public void handleWrite() {
        SocketChannel socketChannel = (SocketChannel) this.key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        byte[] response = ("server workpool处理线程：" + Thread.currentThread().getId() + "server time: " + System.currentTimeMillis() + "\r\n").getBytes();
        buffer.put(response, 0, response.length);
        buffer.flip();
        try {
            socketChannel.write(buffer);
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            selector.wakeup();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
