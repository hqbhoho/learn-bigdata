package com.hqbhoho.bigdata.data.algorithms.recursion;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/27
 */
public class Combinations {

    private List<List<Integer>> res = new ArrayList<>();

    public static void main(String[] args) {
        Combinations combinations = new Combinations();
        ArrayList<Integer> temp = new ArrayList<>();
        combinations.recuCalc(4, 2, temp, 1);
        System.out.println(combinations.res);

    }

    /**
     * 回溯实现
     *
     * @param n
     * @param k
     * @param temp
     * @param start
     */
    public void recuCalc(int n, int k, ArrayList<Integer> temp, int start) {
        // 递归终止条件

        int size = temp.size();
        if (size == k) {
            this.res.add(new ArrayList<>(temp));
            return;
        }

        // 处理当前层

        for (int i = start; i <= n; i++) {
            temp.add(i);
            recuCalc(n, k,  temp, i + 1);
            temp.remove(size);
        }


        // 递归下一层


        // 移除当前层状态

    }
}
