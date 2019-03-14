package com.hqbhoho.bigdata.learnKafka.pojo;

import java.util.List;

/**
 * describe:
 * 定义一个用户类
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class User {
    private int id;
    private String name;
    private List<Item> items;

    public User(int id, String name, List<Item> items) {
        this.id = id;
        this.name = name;
        this.items = items;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public List<Item> getItems() {
        return items;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", items=" + items +
                '}';
    }
}
