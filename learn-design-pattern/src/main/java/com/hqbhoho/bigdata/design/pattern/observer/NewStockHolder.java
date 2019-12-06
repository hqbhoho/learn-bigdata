package com.hqbhoho.bigdata.design.pattern.observer;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/06
 */
public class NewStockHolder implements StockHolder {
    @Override
    public void process(StockEvent stockEvent) {
        System.out.println("NewStockHolder process "+stockEvent);
        if(stockEvent.getPriceChange() > 0){
            System.out.println("NewStockHolder buy stock...");
        }else if(stockEvent.getPriceChange() == 0){
            System.out.println("NewStockHolder no process...");
        }else {
            System.out.println("NewStockHolder sell stock...");
        }
    }
}
