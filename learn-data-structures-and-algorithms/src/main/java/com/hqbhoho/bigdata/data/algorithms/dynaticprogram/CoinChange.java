package com.hqbhoho.bigdata.data.algorithms.dynaticprogram;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/07
 */
public class CoinChange {
    public static void main(String[] args) {
        int[] coins = new int[]{1,5,2};
        int amount = 11;
        System.out.println(dp(coins,amount));
    }

    /**
     * @param coins
     * @param amount
     * @return
     */
    public static int dp(int[] coins, int amount) {

        if (coins == null || coins.length == 0 || amount <= 0) return -1;
        if (Arrays.asList(coins).contains(amount)) return 1;

        int[] amounts = new int[amount + 1];
        Arrays.fill(amounts, amount + 1);
        amounts[0] = 0;
        // 状态转移方程
        // f(n) = f(n-5) + f(n-2) + f(n -1)
        for (int j = 1; j <= amount; j++) {
            for (int i = 0; i < coins.length; i++) {
                if (j - coins[i] < 0) continue;
                amounts[j] = Math.min(amounts[j],amounts[j - coins[i]]+1);
            }
        }
        return amounts[amount] == amount+1?-1:amounts[amount] ;

    }
}
