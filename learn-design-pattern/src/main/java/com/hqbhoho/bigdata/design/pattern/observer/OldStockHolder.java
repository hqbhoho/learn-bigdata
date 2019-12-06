package com.hqbhoho.bigdata.design.pattern.observer;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/06
 */
public class OldStockHolder implements StockHolder {
    @Override
    public void process(StockEvent stockEvent) {
        System.out.println("OldStockHolder process "+stockEvent);
        if(stockEvent.getPriceChange() > 0){
            System.out.println("OldStockHolder buy stock...");
        }else if(stockEvent.getPriceChange() > -10){
            System.out.println("OldStockHolder no process...");
        }else {
            System.out.println("OldStockHolder sell stock...");
        }
    }
}
