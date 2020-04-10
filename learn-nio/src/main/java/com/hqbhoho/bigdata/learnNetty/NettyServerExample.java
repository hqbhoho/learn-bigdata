package com.hqbhoho.bigdata.learnNetty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;

/**
 * describe:
 * Netty Server Example
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/17
 */
public class NettyServerExample {
    public static void main(String[] args) {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        try {
            bootstrap.group(bossGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            ChannelPipeline pipeline = socketChannel.pipeline();
                            // 注意管道的   inbond outbond   channelhandler
                            pipeline.addLast(new LineBasedFrameDecoder(1024));
                            pipeline.addLast(new StringDecoder(Charset.forName("utf-8")));
                            pipeline.addLast(new StringEncoder(Charset.forName("utf-8")));
                            pipeline.addLast(new ServerHandlerExample());
                        }
                    });
            ChannelFuture future = bootstrap.bind(0).sync();
            System.out.println(((InetSocketAddress) future.channel().localAddress()).getPort());

            future.channel().closeFuture().sync();



        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
