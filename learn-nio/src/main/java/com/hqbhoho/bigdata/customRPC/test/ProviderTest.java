package com.hqbhoho.bigdata.customRPC.test;

import com.hqbhoho.bigdata.customRPC.io.TransportConfig;
import com.hqbhoho.bigdata.customRPC.io.TransportFactory;
import com.hqbhoho.bigdata.customRPC.io.netty.NettyTransportFactory;
import com.hqbhoho.bigdata.customRPC.provider.BootStrap;
import com.hqbhoho.bigdata.customRPC.provider.handler.ServiceInvokeHandler;
import com.hqbhoho.bigdata.customRPC.provider.service.impl.UserServiceImpl;
import com.hqbhoho.bigdata.customRPC.registry.register.ZKRegisterFactory;
import com.hqbhoho.bigdata.customRPC.service.UserService;

import java.io.IOException;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class ProviderTest {
    public static void main(String[] args) throws IOException {
        // 注册中心
        ZKRegisterFactory zkRegisterFactory =
                new ZKRegisterFactory("localhost:2181", "/RPC");
        // 服务实例
        ServiceInvokeHandler serviceInvokeHandler = new ServiceInvokeHandler();
        serviceInvokeHandler.register(UserService.class.getName(),new UserServiceImpl());
        // 消息通信

        TransportConfig transportConfig = new TransportConfig();
        TransportFactory transportFactory = new NettyTransportFactory(transportConfig,serviceInvokeHandler);

        BootStrap bootStrap = new BootStrap();
        bootStrap.serviceName("service1")
                .registerFactory(zkRegisterFactory)
                .transportFactory(transportFactory);

        bootStrap.bind("localhost",9999);
    }
}
