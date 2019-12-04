package com.hqbhoho.bigdata.design.pattern.proxy.jdk_dynatic_proxy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class TestDemo {
    public static void main(String[] args) {
        JDKChineseTeacherProxyFactory JDKChineseTeacherProxyFactory = new JDKChineseTeacherProxyFactory();
        ((Teacher) JDKChineseTeacherProxyFactory.newProxyInstance()).teach();
    }
}
