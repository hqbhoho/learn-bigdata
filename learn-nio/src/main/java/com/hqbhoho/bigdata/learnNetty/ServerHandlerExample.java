package com.hqbhoho.bigdata.learnNetty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/17
 */
public class ServerHandlerExample extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ServerHandlerExample.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        LOG.info("接到客户端(ip:{},port:{})的请求，请求内容是: {}", socketAddress.getHostName(), socketAddress.getPort(), msg);
        String response = "Server time: " + System.currentTimeMillis();
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
