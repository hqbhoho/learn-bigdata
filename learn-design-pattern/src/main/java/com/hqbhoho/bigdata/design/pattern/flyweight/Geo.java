package com.hqbhoho.bigdata.design.pattern.flyweight;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/03
 */
public class Geo {
    private int x;
    private int y;

    public Geo(int x, int y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return "Geo{" +
                "x=" + x +
                ", y=" + y +
                '}';
    }
}
