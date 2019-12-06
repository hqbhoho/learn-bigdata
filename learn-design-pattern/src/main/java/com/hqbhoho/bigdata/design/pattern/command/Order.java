package com.hqbhoho.bigdata.design.pattern.command;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class Order {
    private String name;
    private int quantity;

    public Order(String name, int quantity) {
        this.name = name;
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "Order{" +
                "name='" + name + '\'' +
                ", quantity=" + quantity +
                '}';
    }
}
