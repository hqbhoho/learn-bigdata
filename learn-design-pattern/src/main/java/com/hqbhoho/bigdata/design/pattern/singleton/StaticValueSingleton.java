package com.hqbhoho.bigdata.design.pattern.singleton;

/**
 * describe:
 * <p>
 * 饥汉式
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public class StaticValueSingleton {

    private static StaticValueSingleton singleton = new StaticValueSingleton();

    private StaticValueSingleton(){}

    public static StaticValueSingleton newInstance(){
        return singleton;
    }
}
