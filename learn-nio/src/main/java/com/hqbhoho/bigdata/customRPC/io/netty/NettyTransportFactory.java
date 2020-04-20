package com.hqbhoho.bigdata.customRPC.io.netty;

import com.hqbhoho.bigdata.customRPC.io.TransportClient;
import com.hqbhoho.bigdata.customRPC.io.TransportConfig;
import com.hqbhoho.bigdata.customRPC.io.TransportFactory;
import com.hqbhoho.bigdata.customRPC.io.TransportServer;
import io.netty.channel.ChannelHandler;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class NettyTransportFactory implements TransportFactory {
    private TransportConfig transportConfig;
    private ChannelHandler[] handlers;

    public NettyTransportFactory(TransportConfig transportConfig,ChannelHandler... handlers) {
        this.transportConfig = transportConfig;
        this.handlers = handlers;
    }

    @Override
    public TransportServer newTransportServer() {
        String ip = (String)transportConfig.getConfig("SERVERIP");
        Integer port = (Integer) transportConfig.getConfig("SERVERPORT");
        return new NettyTransportServer(ip,port,handlers);

    }

    @Override
    public TransportClient newTransportClient() {
        String ip = (String)transportConfig.getConfig("SERVERIP");
        Integer port = (Integer) transportConfig.getConfig("SERVERPORT");
        return new NettyTransportClient(ip,port,handlers);
    }

    @Override
    public TransportConfig getTransportConfig() {
        return this.transportConfig;
    }
}
