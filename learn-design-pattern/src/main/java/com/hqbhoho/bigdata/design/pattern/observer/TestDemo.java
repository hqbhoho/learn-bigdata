package com.hqbhoho.bigdata.design.pattern.observer;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/06
 */
public class TestDemo {
    public static void main(String[] args) {

        StockCompany stockCompany = new StockCompany();
        StockHolder newStockHolder = new NewStockHolder();
        StockHolder oldStockHolder = new OldStockHolder();
        stockCompany.addStockHolder(newStockHolder);
        stockCompany.addStockHolder(oldStockHolder);

        stockCompany.process(new StockEvent(5));
        System.out.println("================================");
        stockCompany.process(new StockEvent(-5));
        System.out.println("================================");
        stockCompany.process(new StockEvent(-15));


    }
}
