package com.hqbhoho.bigdata.customRPC.consumer.callback;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public interface ServiceCallBack<T> {
    public void success(T t);
    public void fail(T t);
}
