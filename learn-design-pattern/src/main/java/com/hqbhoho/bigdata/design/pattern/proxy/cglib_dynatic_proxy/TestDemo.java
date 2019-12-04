package com.hqbhoho.bigdata.design.pattern.proxy.cglib_dynatic_proxy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class TestDemo {
    public static void main(String[] args) {
        CglibChineseTeacherProxyFactory cglibChineseTeacherProxyFactory = new CglibChineseTeacherProxyFactory();
        ((ChineseTeacher) cglibChineseTeacherProxyFactory.newProxyInstance()).teach();
    }
}
