package com.hqbhoho.bigdata.learnNetty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.nio.charset.Charset;


/**
 * describe:
 * Netty Client Example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/19
 */
public class NettyClientExample {
    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new StringDecoder(Charset.forName("utf-8")));
                        pipeline.addLast(new StringEncoder(Charset.forName("utf-8")));
                        pipeline.addLast(new ClientHandlerExample());
                    }
                });
        ChannelFuture future = bootstrap.connect("localhost", 9999).sync();
        future.channel().closeFuture().sync();

    }
}
