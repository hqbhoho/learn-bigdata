package com.hqbhoho.bigdata.learnNio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/17
 */
public class ClientRequestHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestHandler.class);

    private Selector selector;

    private ByteBuffer buffer = ByteBuffer.allocate(1024);

    public ClientRequestHandler(Selector selector) {
        this.selector = selector;
    }

    public void handleConnect(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        try {
            InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
            LOG.info("<========>客户端上线了,已与服务器端(ip:{},port:{})建立好连接。", remoteAddress.getHostName(), remoteAddress.getPort());
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            if (socketChannel.isConnectionPending()) socketChannel.finishConnect();
            selector.wakeup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void handleRead(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        buffer.clear();
        try {
            socketChannel.read(buffer);
            InetSocketAddress remoteAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
            LOG.info("<========>服务端(ip:{},port:{}),返回了响应,响应内容:{},响应到达时间:{}",
                    remoteAddress.getHostName(), remoteAddress.getPort(),
                    new String(buffer.array()), System.currentTimeMillis());
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void handleWrite(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        buffer.clear();
        System.out.println("请输入发送给服务器的内容：");
        Scanner scan = new Scanner(System.in);
        byte[] message = scan.nextLine().getBytes();
        buffer.put(message, 0, message.length);
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
