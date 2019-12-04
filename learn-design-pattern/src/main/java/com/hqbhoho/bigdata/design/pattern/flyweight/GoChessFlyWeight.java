package com.hqbhoho.bigdata.design.pattern.flyweight;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class GoChessFlyWeight implements ChessFlyWeight {

    private String color;

    public GoChessFlyWeight(String color) {
        this.color = color;
    }

    @Override
    public String toString() {
        return "GoChess{" +
                "color='" + color + '\'' +
                " ,hashcode= " + this.hashCode() +
                '}';
    }

    @Override
    public void display(Geo geo) {
        System.out.println(this + " put on " + geo);
    }
}
