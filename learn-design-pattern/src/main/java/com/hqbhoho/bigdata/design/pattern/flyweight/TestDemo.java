package com.hqbhoho.bigdata.design.pattern.flyweight;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class TestDemo {
    public static void main(String[] args) {
        GoChessFlyWeightFactory goChessFlyWeightFactory = new GoChessFlyWeightFactory();
        ChessFlyWeight white1 = goChessFlyWeightFactory.getChessFlyWeight("white");
        white1.display(new Geo(1,1));
        ChessFlyWeight white2 = goChessFlyWeightFactory.getChessFlyWeight("white");
        white2.display(new Geo(2,2));
        ChessFlyWeight white3 = goChessFlyWeightFactory.getChessFlyWeight("white");
        white3.display(new Geo(3,3));
        
        ChessFlyWeight black1 = goChessFlyWeightFactory.getChessFlyWeight("black");
        black1.display(new Geo(4,4));
        ChessFlyWeight black2 = goChessFlyWeightFactory.getChessFlyWeight("black");
        black2.display(new Geo(5,5));
        ChessFlyWeight black3 = goChessFlyWeightFactory.getChessFlyWeight("black");
        black3.display(new Geo(6,6));
        
    }
}
