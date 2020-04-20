package com.hqbhoho.bigdata.customRPC.registry.register;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class ZKRegisterFactory implements RegisterFactory {
    private String connectionInfo;
    private String namespaece;

    public ZKRegisterFactory(String connectionInfo, String namespaece) {
        this.connectionInfo = connectionInfo;
        this.namespaece = namespaece;
    }

    @Override
    public RegisterClient newInstance() {
        return new ZKRigesterClient(this.connectionInfo,this.namespaece);
    }
}
