package com.hqbhoho.bigdata.design.pattern.singleton;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public class StaticInnerClassSingleton {

    private StaticInnerClassSingleton() {
    }

    static class Builder{
        private static StaticInnerClassSingleton singleton = new StaticInnerClassSingleton();
    }

    public static StaticInnerClassSingleton newInstance(){
        return Builder.singleton;
    }
}
