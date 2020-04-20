package com.hqbhoho.bigdata.customRPC.io;

import com.hqbhoho.bigdata.customRPC.consumer.callback.ServiceCallBack;

public interface TransportClient {
    RPCResponse postRequest(RPCRequest request,ServiceCallBack<RPCResponse> serviceCallBack);

    void init();
}
