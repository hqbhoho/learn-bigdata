package com.hqbhoho.bigdata.design.pattern.bridge;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class QQ implements Software {
    @Override
    public void action() {
        System.out.println("You have install QQ, You can share your life in QQ...");
    }
}
