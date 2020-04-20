package com.hqbhoho.bigdata.customRPC.provider;

import com.hqbhoho.bigdata.customRPC.io.TransportConfig;
import com.hqbhoho.bigdata.customRPC.io.TransportFactory;
import com.hqbhoho.bigdata.customRPC.registry.ServiceProviderInfo;
import com.hqbhoho.bigdata.customRPC.registry.register.RegisterFactory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class BootStrap {
    private RegisterFactory registerFactory;
    private TransportFactory transportFactory;
    private String serviceName;


    public BootStrap() {
    }

    public BootStrap registerFactory(RegisterFactory registerFactory) {
        this.registerFactory = registerFactory;
        return this;
    }

    public BootStrap serviceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public BootStrap transportFactory(TransportFactory transportFactory) {
        this.transportFactory = transportFactory;
        return this;
    }

    public void bind(String ip, int port) {

        try {
            ServiceProviderInfo serviceProviderInfo = new ServiceProviderInfo(ip, port);
            //  完成服务注册
            registerFactory.newInstance().register(this.serviceName, serviceProviderInfo);

            // 启动netty server
            TransportConfig transportConfig = transportFactory.getTransportConfig();
            transportConfig.setConfig("SERVERIP",ip);
            transportConfig.setConfig("SERVERPORT",port);
            transportFactory.newTransportServer().start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
