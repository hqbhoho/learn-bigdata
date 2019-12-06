package com.hqbhoho.bigdata.design.pattern.observer;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/06
 */
public class StockCompany {
    private List<StockHolder> stockHolders;

    public StockCompany() {
        this.stockHolders = new ArrayList<>();
    }

    public void addStockHolder(StockHolder stockHolder) {
        stockHolders.add(stockHolder);
    }

    public void removeStockHolder(StockHolder stockHolder) {
        stockHolders.remove(stockHolder);
    }

    public void process(StockEvent stockEvent){
        stockHolders.stream().forEach(sh -> sh.process(stockEvent));
    }
}
