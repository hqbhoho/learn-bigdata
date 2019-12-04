package com.hqbhoho.bigdata.design.pattern.strategy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class TestDemo {
    public static void main(String[] args) {
        QuoteStrategyContext quoteStrategyContext = new QuoteStrategyContext();
        QuoteStrategy newCustomerQuoteStrategy = new NewCustomerQuoteStrategy();
        quoteStrategyContext.setQuoteStrategy(newCustomerQuoteStrategy);
        System.out.println(quoteStrategyContext.getPrice());
        System.out.println("=================================");
        QuoteStrategy oldCustomerQuoteStrategy = new OldCustomerQuoteStrategy();
        quoteStrategyContext.setQuoteStrategy(oldCustomerQuoteStrategy);
        System.out.println(quoteStrategyContext.getPrice());
        System.out.println("=================================");
        QuoteStrategy vipCustomerQuoteStrategy = new VIPCustomerQuoteStrategy();
        quoteStrategyContext.setQuoteStrategy(vipCustomerQuoteStrategy);
        System.out.println(quoteStrategyContext.getPrice());
    }
}
