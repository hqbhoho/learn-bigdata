package com.hqbhoho.bigdata.design.pattern.abstract_factory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public abstract class AbstractPhone implements Phone {
    @Override
    public void call() {
        System.out.println("我能打电话");
    }
}
