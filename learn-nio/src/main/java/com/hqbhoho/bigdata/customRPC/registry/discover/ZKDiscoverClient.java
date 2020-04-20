package com.hqbhoho.bigdata.customRPC.registry.discover;

import com.alibaba.fastjson.JSON;
import com.hqbhoho.bigdata.customRPC.registry.ServiceProviderInfo;
import com.hqbhoho.bigdata.customRPC.registry.register.ZKRigesterClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class ZKDiscoverClient implements DiscoverClient {

    private static final Logger LOG = LoggerFactory.getLogger(ZKRigesterClient.class);
    private CuratorFramework client;
    private String namespaece;

    public ZKDiscoverClient(String connectionInfo, String namespaece) {
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
    }


    @Override
    public ServiceProviderInfo getService(String serviceName) {
        String registerParent = this.namespaece + "/" + serviceName;
        try {
            List<String> nodes = client.getChildren().forPath(registerParent);
            Random rand = new Random();
            int index = rand.nextInt(nodes.size());
            byte[] serviceInfoBytes = client.getData().forPath(registerParent+"/"+nodes.get(index));
            return JSON.parseObject(serviceInfoBytes, ServiceProviderInfo.class);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return null;
    }
}
