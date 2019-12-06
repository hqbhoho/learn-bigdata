package com.hqbhoho.bigdata.design.pattern.command;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class SellCommand implements OrderCommand{

    private OrderOperator orderOperator;

    public SellCommand(OrderOperator orderOperator) {
        this.orderOperator = orderOperator;
    }

    @Override
    public void execute(Order order) {
        orderOperator.sell(order);
    }
}
