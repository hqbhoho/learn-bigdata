package com.hqbhoho.bigdata.customRPC.registry.register;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/19
 */
public interface RegisterFactory {
    RegisterClient newInstance();
}
