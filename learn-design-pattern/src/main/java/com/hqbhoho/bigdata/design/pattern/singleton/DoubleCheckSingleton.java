package com.hqbhoho.bigdata.design.pattern.singleton;

/**
 * describe:
 * <p>
 * 双重检查（单例）
 * <p>
 * Double Check + volatile
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/26
 */
public class DoubleCheckSingleton {

    private static volatile DoubleCheckSingleton singleTon;

    private DoubleCheckSingleton() {
    }

    public static DoubleCheckSingleton newInstance() {
        if (singleTon == null) {
            synchronized (DoubleCheckSingleton.class) {
                if (singleTon == null) {
                    singleTon = new DoubleCheckSingleton();
                }
            }
        }
        return singleTon;
    }
}
