package com.hqbhoho.bigdata.design.pattern.proxy.static_proxy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class TestDemo {
    public static void main(String[] args) {
        Teacher chineseTeacherProxy = new ChineseTeacherProxy();
        chineseTeacherProxy.teach();
    }
}
