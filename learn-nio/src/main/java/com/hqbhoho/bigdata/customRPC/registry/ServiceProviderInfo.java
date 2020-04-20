package com.hqbhoho.bigdata.customRPC.registry;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class ServiceProviderInfo {
    private String ip;
    private int port;
//    private Enum protocol;

    public ServiceProviderInfo() {
    }

    public ServiceProviderInfo(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "ServiceProviderInfo{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }
}
