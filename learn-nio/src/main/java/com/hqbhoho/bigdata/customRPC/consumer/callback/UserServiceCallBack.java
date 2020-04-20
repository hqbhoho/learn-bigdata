package com.hqbhoho.bigdata.customRPC.consumer.callback;

import com.hqbhoho.bigdata.customRPC.io.RPCResponse;
import com.hqbhoho.bigdata.customRPC.service.pojo.User;

import java.util.Optional;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public class UserServiceCallBack implements ServiceCallBack<RPCResponse> {
    @Override
    public void success(RPCResponse rpcResponse) {
        int status = rpcResponse.getStatus();
        User user = (User)rpcResponse.getData();
        Optional.ofNullable("CallBack : "+status).ifPresent(System.err::println);
        Optional.ofNullable("CallBack : "+user).ifPresent(System.err::println);
    }

    @Override
    public void fail(RPCResponse rpcResponse) {
        // TODO
    }
}
