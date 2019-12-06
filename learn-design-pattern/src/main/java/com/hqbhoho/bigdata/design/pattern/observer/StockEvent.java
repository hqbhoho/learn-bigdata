package com.hqbhoho.bigdata.design.pattern.observer;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/06
 */
public class StockEvent {
    private int priceChange;

    public StockEvent(int priceChange) {
        this.priceChange = priceChange;
    }

    @Override
    public String toString() {
        return "StockEvent{" +
                "priceChange=" + priceChange +
                '}';
    }

    public int getPriceChange() {
        return priceChange;
    }
}
