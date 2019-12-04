package com.hqbhoho.bigdata.design.pattern.strategy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class VIPCustomerQuoteStrategy implements QuoteStrategy {
    @Override
    public Double getQuotePrice() {
        System.out.println("VIPCustomer 50% discount");
        return 50.00;
    }
}
