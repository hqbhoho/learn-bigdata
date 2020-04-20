package com.hqbhoho.bigdata.customRPC.registry.register;

import com.alibaba.fastjson.JSON;
import com.hqbhoho.bigdata.customRPC.registry.ServiceProviderInfo;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * describe:
 * <p>
 * 向zookeeper注册服务
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class ZKRigesterClient implements RegisterClient {
    private CuratorFramework client;
    private String namespaece;
    private volatile boolean registered;

    public ZKRigesterClient(String connectionInfo, String namespaece) {
        // 创建zookeeper客户端
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.client =
                CuratorFrameworkFactory.newClient(
                        connectionInfo,
                        5000,
                        3000,
                        retryPolicy);
        this.client.start();
        this.namespaece = namespaece;
        this.registered = false;
    }

    @Override
    public void register(String serviceName, ServiceProviderInfo serviceProviderInfo) {
        if (this.registered == true) {
            System.err.println(serviceName + " has be registerd....");
            return;
        }
        String registerParent = this.namespaece + "/" + serviceName;
        try {
            String serviceInfo = JSON.toJSONString(serviceProviderInfo);
            Stat parent = client.checkExists().forPath(registerParent);
            if (parent == null) {
                client.create()
                        .creatingParentContainersIfNeeded()
                        .forPath(registerParent);
            }

            client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(registerParent + "/node", serviceInfo.getBytes());
            this.registered = true;
        } catch (Exception e) {
            e.printStackTrace();
            this.registered = false;
        }
    }
}
