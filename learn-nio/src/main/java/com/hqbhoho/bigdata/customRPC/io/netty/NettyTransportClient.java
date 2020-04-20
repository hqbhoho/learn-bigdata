package com.hqbhoho.bigdata.customRPC.io.netty;

import com.hqbhoho.bigdata.customRPC.consumer.callback.ServiceCallBack;
import com.hqbhoho.bigdata.customRPC.io.RPCRequest;
import com.hqbhoho.bigdata.customRPC.io.RPCResponse;
import com.hqbhoho.bigdata.customRPC.io.TransportClient;
import com.hqbhoho.bigdata.customRPC.io.netty.protocol.MarshallingCodeCFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class NettyTransportClient implements TransportClient {
    // 异步回调
    private ConcurrentHashMap<String, ServiceCallBack<RPCResponse>> callbacks;
    // 同步返回
    private ConcurrentHashMap<String, SynchronousQueue<RPCResponse>> responses;
    // 发送请求队列
    private LinkedBlockingQueue<RPCRequest> pengingRequests;
    private String ip;
    private int port;
    private Channel channel;
    private ChannelHandler[] handlers;

    private volatile boolean init;

    public NettyTransportClient(String ip, int port, ChannelHandler... handlers) {
        this.ip = ip;
        this.port = port;
        this.handlers = handlers;
        this.callbacks = new ConcurrentHashMap<>();
        this.responses = new ConcurrentHashMap<>();
        this.pengingRequests = new LinkedBlockingQueue<>();
        this.init = false;
    }

    @Override
    public void init() {
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {

                EventLoopGroup workerGroup = new NioEventLoopGroup();
                Bootstrap bootstrap = new Bootstrap();

                bootstrap.group(workerGroup)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel socketChannel) throws Exception {
                                ChannelPipeline pipeline = socketChannel.pipeline();
                                pipeline.addLast(MarshallingCodeCFactory.buildMarshallingEncoder());
                                pipeline.addLast(MarshallingCodeCFactory.buildMarshallingDecoder());
                                if (handlers != null) {
                                    pipeline.addLast(handlers);
                                }

                                pipeline.addLast(new ResponseProcessHandler(callbacks, responses,pengingRequests));
                            }
                        });
                ChannelFuture future = bootstrap.connect(this.ip, this.port).sync();
                this.channel = future.channel();
                this.init = true;
                latch.countDown();
                future.channel().closeFuture().sync();

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        try {
            latch.await();
            System.out.println("NettyTransportClient has been inited...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public RPCResponse postRequest(RPCRequest request, ServiceCallBack<RPCResponse> serviceCallBack) {
        if (!init) {
            throw new RuntimeException("NettyTransportClient has not been inited...");
        }
        RPCResponse response = null;
//        this.channel.pipeline().writeAndFlush(request);
        pengingRequests.add(request);
        callbacks.put(request.getId(), serviceCallBack);
        SynchronousQueue<RPCResponse> queue = new SynchronousQueue<>();
        responses.put(request.getId(), queue);
        try {
            response = queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return response;
    }

    static class ResponseProcessHandler extends ChannelInboundHandlerAdapter {
        // 异步回调
        private ConcurrentHashMap<String, ServiceCallBack<RPCResponse>> callbacks;
        // 同步返回
        private ConcurrentHashMap<String, SynchronousQueue<RPCResponse>> responses;
        // 发送请求队列
        private LinkedBlockingQueue<RPCRequest> pengingRequests;

        public ResponseProcessHandler(ConcurrentHashMap<String, ServiceCallBack<RPCResponse>> callbacks,
                                      ConcurrentHashMap<String, SynchronousQueue<RPCResponse>> responses,
                                      LinkedBlockingQueue<RPCRequest> pengingRequests) {
            this.callbacks = callbacks;
            this.responses = responses;
            this.pengingRequests = pengingRequests;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            new Thread(()->{
                try {
                    RPCRequest request = this.pengingRequests.take();
                    ctx.channel().pipeline().writeAndFlush(request);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();

        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

            RPCResponse response = (RPCResponse) msg;
            String requestId = response.getRequestId();
            int status = response.getStatus();
            ServiceCallBack<RPCResponse> callBack = callbacks.get(requestId);
            if (status == 200) {
                callBack.success(response);
            } else {
                callBack.fail(response);
            }
            responses.get(requestId).put(response);
        }

    }
}
