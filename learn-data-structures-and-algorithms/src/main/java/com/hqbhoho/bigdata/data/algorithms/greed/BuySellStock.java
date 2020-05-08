package com.hqbhoho.bigdata.data.algorithms.greed;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/08
 */
public class BuySellStock {
    public static void main(String[] args) {
        int[] prices = new int[]{7,1,5,3,6,4};
        System.out.println(greedCalc(prices));

    }

    public static int greedCalc(int[] prices){
        int maxProfit = 0;
        for(int i = 0 ;i<prices.length-1;i++){
            if(prices[i+1] > prices[i]){
                maxProfit += prices[i+1]-prices[i];
            }
        }
        return maxProfit;
    }
}
