package com.hqbhoho.bigdata.design.pattern.strategy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class OldCustomerQuoteStrategy implements QuoteStrategy {
    @Override
    public Double getQuotePrice() {
        System.out.println("OldCustomer 30% discount");
        return 70.00;
    }
}
