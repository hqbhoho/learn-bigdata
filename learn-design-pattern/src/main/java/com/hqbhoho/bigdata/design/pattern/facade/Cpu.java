package com.hqbhoho.bigdata.design.pattern.facade;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class Cpu {
    public void onStart(){
        System.out.println("Turn on CPU...");
    }
    public void onStop(){
        System.out.println("Turn off CPU...");
    }
}
