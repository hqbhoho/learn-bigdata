package com.hqbhoho.bigdata.design.pattern.strategy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class NewCustomerQuoteStrategy implements QuoteStrategy {
    @Override
    public Double getQuotePrice() {
        System.out.println("NewCustomer no discount");
        return 100.00;
    }
}
