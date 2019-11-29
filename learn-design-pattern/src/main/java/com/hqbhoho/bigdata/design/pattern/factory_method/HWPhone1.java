package com.hqbhoho.bigdata.design.pattern.factory_method;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/28
 */
public class HWPhone1 extends AbstractPhone {
    private String name;
    private String brand;
    private Double price;

    public HWPhone1(String name, String brand, Double price) {
        this.name = name;
        this.brand = brand;
        this.price = price;
    }

    @Override
    public String toString() {
        return "HWPhone1{" +
                "name='" + name + '\'' +
                ", brand='" + brand + '\'' +
                ", price=" + price +
                '}';
    }

    @Override
    public void specialAction() {
        System.out.println("我能拍照...");
    }
}
