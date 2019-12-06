package com.hqbhoho.bigdata.design.pattern.command;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class TestDemo {
    public static void main(String[] args) {
        OrderOperator orderOperator = new OrderOperator("hqbhoho");

        Order order = new Order("000001", 1000);


        OrderCommand buyCommand = new BuyCommand(orderOperator);
        OrderCommand sellCommand = new SellCommand(orderOperator);


        buyCommand.execute(order);
        sellCommand.execute(order);

    }
}
