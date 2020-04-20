package com.hqbhoho.bigdata.customRPC.registry.discover;

import com.hqbhoho.bigdata.customRPC.registry.ServiceProviderInfo;

public interface DiscoverClient {

    ServiceProviderInfo getService(String serviceName);

}
