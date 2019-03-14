package com.hqbhoho.bigdata.learnKafka.pojo;

/**
 * describe:
 * 定义一个商品类
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class Item {
    private int id;
    private String name;
    private double price;

    public Item(int id, String name, double price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "Item{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", price=" + price +
                '}';
    }
}
