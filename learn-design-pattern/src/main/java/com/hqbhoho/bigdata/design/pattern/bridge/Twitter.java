package com.hqbhoho.bigdata.design.pattern.bridge;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/02
 */
public class Twitter implements Software {
    @Override
    public void action() {
        System.out.println("You have install Twitter,You can share your life in Twitter...");
    }
}
