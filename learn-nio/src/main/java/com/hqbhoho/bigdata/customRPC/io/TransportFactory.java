package com.hqbhoho.bigdata.customRPC.io;

public interface TransportFactory {

    TransportServer newTransportServer();

    TransportClient newTransportClient();

    TransportConfig getTransportConfig();
}
