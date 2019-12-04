package com.hqbhoho.bigdata.design.pattern.template_method;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class TestDemo {
    public static void main(String[] args) {
        Game nba2K19 = new NBA2K19();
        Game dota = new Dota();
        nba2K19.playGame();
        System.out.println("===================");
        dota.playGame();
    }
}
