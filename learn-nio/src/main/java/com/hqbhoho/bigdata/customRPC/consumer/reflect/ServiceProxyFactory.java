package com.hqbhoho.bigdata.customRPC.consumer.reflect;

import com.hqbhoho.bigdata.customRPC.consumer.callback.ServiceCallBack;
import com.hqbhoho.bigdata.customRPC.io.*;
import com.hqbhoho.bigdata.customRPC.registry.ServiceProviderInfo;
import com.hqbhoho.bigdata.customRPC.registry.discover.DiscoverClient;
import com.hqbhoho.bigdata.customRPC.registry.discover.DiscoverFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.UUID;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class ServiceProxyFactory implements InvocationHandler {

    private DiscoverFactory discoverFactory;
    private DiscoverClient discoverClient;

    private TransportFactory transportFactory;
    private TransportClient transportClient;

    private Class serviceInterface;

    private ServiceCallBack<RPCResponse> serviceCallBack;
    private String serviceName;

    public ServiceProxyFactory(DiscoverFactory discoverFactory, TransportFactory transportFactory,
                               Class<?> serviceInterface, ServiceCallBack<RPCResponse> serviceCallBack,
                               String serviceName
    ) {
        this.serviceInterface = serviceInterface;
        this.discoverFactory = discoverFactory;
        this.transportFactory = transportFactory;
        this.serviceCallBack = serviceCallBack;
        this.serviceName = serviceName;
    }

    public Object newProxyInstance() {
        this.discoverClient = discoverFactory.newInstance();
        return Proxy.newProxyInstance(this.serviceInterface.getClassLoader(),
                new Class[]{this.serviceInterface},
                this
        );

    }

    @Override
    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {

        RPCRequest request = new RPCRequest();
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();

        request.setId(UUID.randomUUID().toString());
        request.setClassName(this.serviceInterface.getName());
        request.setMethodName(methodName);
        request.setParameters(objects);
        request.setParameterTypes(parameterTypes);

        // 获取远程服务信息
        ServiceProviderInfo service1 = this.discoverClient.getService(serviceName);
        // 远程服务建立连接
        TransportConfig transportConfig = transportFactory.getTransportConfig();
        transportConfig.setConfig("SERVERIP", service1.getIp());
        transportConfig.setConfig("SERVERPORT", service1.getPort());
        TransportClient transportClient = transportFactory.newTransportClient();
        transportClient.init();
        RPCResponse response = transportClient.postRequest(request, serviceCallBack);
        if (response.getStatus() == 200) {
            return response.getData();
        } else {
            return null;
        }

    }

}
