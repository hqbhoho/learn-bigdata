package com.hqbhoho.bigdata.design.pattern.command;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/05
 */
public class BuyCommand implements OrderCommand {

    private OrderOperator orderOperator;

    public BuyCommand(OrderOperator orderOperator) {
        this.orderOperator = orderOperator;
    }

    @Override
    public void execute(Order order) {
        orderOperator.buy(order);
    }
}
