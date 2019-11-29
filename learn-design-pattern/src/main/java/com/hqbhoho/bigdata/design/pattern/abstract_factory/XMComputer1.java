package com.hqbhoho.bigdata.design.pattern.abstract_factory;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/29
 */
public class XMComputer1 extends AbstractComputer{
    private String name;
    private String brand;
    private Double price;

    public XMComputer1(String name, String brand, Double price) {
        this.name = name;
        this.brand = brand;
        this.price = price;
    }

    @Override
    public String toString() {
        return "XMComputer1{" +
                "name='" + name + '\'' +
                ", brand='" + brand + '\'' +
                ", price=" + price +
                '}';
    }
}
