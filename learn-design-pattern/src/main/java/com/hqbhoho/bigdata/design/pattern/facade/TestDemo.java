package com.hqbhoho.bigdata.design.pattern.facade;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class TestDemo {
    public static void main(String[] args) {
        Computer computer = new Computer();
        computer.start();
        System.out.println("====================================");
        computer.stop();
    }
}
