package com.hqbhoho.bigdata.customRPC.test;

import com.hqbhoho.bigdata.customRPC.consumer.callback.ServiceCallBack;
import com.hqbhoho.bigdata.customRPC.consumer.callback.UserServiceCallBack;
import com.hqbhoho.bigdata.customRPC.consumer.reflect.ServiceProxyFactory;
import com.hqbhoho.bigdata.customRPC.io.RPCResponse;
import com.hqbhoho.bigdata.customRPC.io.TransportConfig;
import com.hqbhoho.bigdata.customRPC.io.TransportFactory;
import com.hqbhoho.bigdata.customRPC.io.netty.NettyTransportFactory;
import com.hqbhoho.bigdata.customRPC.registry.discover.DiscoverFactory;
import com.hqbhoho.bigdata.customRPC.registry.discover.ZKDiscoverFactory;
import com.hqbhoho.bigdata.customRPC.service.UserService;
import com.hqbhoho.bigdata.customRPC.service.pojo.User;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class ConsumerTest {
    public static void main(String[] args) {

        DiscoverFactory discoverFactory = new ZKDiscoverFactory("localhost:2181", "/RPC");
        TransportConfig transportConfig = new TransportConfig();
        TransportFactory transportFactory = new NettyTransportFactory(transportConfig,null);
        Class serviceInterface = UserService.class;
        ServiceCallBack<RPCResponse> serviceCallBack = new UserServiceCallBack();
        String serviceName = "service1";

        UserService userService = (UserService)new ServiceProxyFactory(discoverFactory,transportFactory,serviceInterface,serviceCallBack,serviceName).newProxyInstance();
        User user1 = userService.getUserById(1);
        System.err.println("Response：  "+user1);
        User user2 = userService.getUserByName("hqbhoho2");
        System.err.println("Response：  "+user2);
    }
}
