package com.hqbhoho.bigdata.design.pattern.strategy;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/04
 */
public class QuoteStrategyContext {

    private QuoteStrategy quoteStrategy;

    public QuoteStrategyContext() {
    }

    public QuoteStrategyContext(QuoteStrategy quoteStrategy) {
        this.quoteStrategy = quoteStrategy;
    }

    public void setQuoteStrategy(QuoteStrategy quoteStrategy) {
        this.quoteStrategy = quoteStrategy;
    }

    public Double getPrice(){
        return quoteStrategy.getQuotePrice();
    }
}
