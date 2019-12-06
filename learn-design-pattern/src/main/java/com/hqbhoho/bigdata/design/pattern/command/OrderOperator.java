package com.hqbhoho.bigdata.design.pattern.command;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class OrderOperator {

    private String name;

    public OrderOperator(String name) {
        this.name = name;
    }

    public void buy(Order order){
        System.out.println("=================buy==================");
        System.out.println("Operator: "+this.name);
        System.out.println("Order: "+order);
    }

    public void sell(Order order){
        System.out.println("=================sell==================");
        System.out.println("Operator: "+this.name);
        System.out.println("Order: "+order);
    }
}
