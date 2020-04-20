package com.hqbhoho.bigdata.customRPC.registry.discover;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class ZKDiscoverFactory implements DiscoverFactory {
    private String connectionInfo;
    private String namespaece;

    public ZKDiscoverFactory(String connectionInfo, String namespaece) {
        this.connectionInfo = connectionInfo;
        this.namespaece = namespaece;
    }

    @Override
    public DiscoverClient newInstance() {
        return new ZKDiscoverClient(this.connectionInfo,this.namespaece);
    }
}
