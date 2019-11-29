package com.hqbhoho.bigdata.design.pattern.abstract_factory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public class HWPhone2 extends AbstractPhone {
    private String name;
    private String brand;
    private Double price;

    public HWPhone2(String name, String brand, Double price) {
        this.name = name;
        this.brand = brand;
        this.price = price;
    }

    @Override
    public String toString() {
        return "HWPhone2{" +
                "name='" + name + '\'' +
                ", brand='" + brand + '\'' +
                ", price=" + price +
                '}';
    }

    @Override
    public void specialAction() {
        System.out.println("我能播放电影...");
    }
}
