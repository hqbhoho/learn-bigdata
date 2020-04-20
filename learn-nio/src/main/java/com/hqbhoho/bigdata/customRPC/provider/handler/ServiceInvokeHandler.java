package com.hqbhoho.bigdata.customRPC.provider.handler;

import com.hqbhoho.bigdata.customRPC.io.RPCRequest;
import com.hqbhoho.bigdata.customRPC.io.RPCResponse;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.HashMap;


/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
@ChannelHandler.Sharable
public class ServiceInvokeHandler extends ChannelInboundHandlerAdapter {
    private HashMap<String, Object> registry = new HashMap<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RPCResponse response = new RPCResponse();
        String id = null;
        try {
            RPCRequest request = (RPCRequest) msg;
            InetSocketAddress socketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
            System.err.printf("接到客户端(ip:%s,port:%d)的请求，请求内容是: %s\n", socketAddress.getHostName(), socketAddress.getPort(), msg);
            id = request.getId();
            String className = request.getClassName();
            String methodName = request.getMethodName();
            Class<?>[] parameterTypes = request.getParameterTypes();
            Object[] parameters = request.getParameters();
            // 反射调用
            Class<?> userClass = Class.forName(className);
            Object instance = registry.get(className);
            Method method = userClass.getMethod(methodName, parameterTypes);
            Object result = method.invoke(instance, parameters);

            response.setData(result);
            response.setRequestId(id);
            response.setStatus(200);
            response.setErrorMessage(null);
        } catch (Exception e) {
            e.printStackTrace();
            response.setData(null);
            response.setRequestId(id);
            response.setStatus(500);
            response.setErrorMessage(e.getMessage());
        } finally {
            ctx.channel().pipeline().writeAndFlush(response);
        }
    }


    public void register(String key, Object value) {
        registry.put(key, value);
    }
}
