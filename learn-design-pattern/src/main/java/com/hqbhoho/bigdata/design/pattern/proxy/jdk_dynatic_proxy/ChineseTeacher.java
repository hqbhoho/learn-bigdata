package com.hqbhoho.bigdata.design.pattern.proxy.jdk_dynatic_proxy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class ChineseTeacher implements Teacher {
    @Override
    public void teach() {
        System.out.println("现在开始上语文课...");
    }
}
