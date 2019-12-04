package com.hqbhoho.bigdata.design.pattern.template_method;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class NBA2K19 extends Game {
    @Override
    protected void open() {
        System.out.println("open game NBA2K19......");
    }

    @Override
    protected void startPlay() {
        System.out.println("begin to play game NBA2K19......");
    }

    @Override
    protected void stopPlay() {
        System.out.println("stop to play game NBA2K19......");
    }

    @Override
    protected boolean canReadHistory() {
        return true;
    }
}
