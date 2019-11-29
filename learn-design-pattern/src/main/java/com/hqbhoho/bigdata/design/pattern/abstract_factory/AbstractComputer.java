package com.hqbhoho.bigdata.design.pattern.abstract_factory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/29
 */
public abstract class AbstractComputer implements Computer {
    @Override
    public void code() {
        System.out.println("我能敲代码...");
    }
}
