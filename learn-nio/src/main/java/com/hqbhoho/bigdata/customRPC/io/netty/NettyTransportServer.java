package com.hqbhoho.bigdata.customRPC.io.netty;

import com.hqbhoho.bigdata.customRPC.io.TransportServer;
import com.hqbhoho.bigdata.customRPC.io.netty.protocol.MarshallingCodeCFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class NettyTransportServer implements TransportServer {

    private String ip;
    private int port;
    private ChannelHandler[] handlers;
    private boolean inited;

    public NettyTransportServer(String ip, int port, ChannelHandler[] handlers) {
        this.ip = ip;
        this.port = port;
        this.handlers = handlers;
        this.inited = false;
    }

    @Override
    public void start() {
        if (inited == true) {
            System.err.println("RPC Netty Server has been started.......");
        }
        try {
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap();

            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            ChannelPipeline pipeline = socketChannel.pipeline();
                            pipeline.addLast(MarshallingCodeCFactory.buildMarshallingEncoder());
                            pipeline.addLast(MarshallingCodeCFactory.buildMarshallingDecoder());
                            pipeline.addLast(handlers);
                        }
                    });
            ChannelFuture future = bootstrap.bind(new InetSocketAddress(this.ip, this.port)).sync();

            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
