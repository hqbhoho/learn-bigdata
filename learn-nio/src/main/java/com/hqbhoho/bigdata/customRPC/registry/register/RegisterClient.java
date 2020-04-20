package com.hqbhoho.bigdata.customRPC.registry.register;

import com.hqbhoho.bigdata.customRPC.registry.ServiceProviderInfo;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public interface RegisterClient {
    // 向注册中心注册服务信息
    void register(String serviceName, ServiceProviderInfo serviceInfo);
}
