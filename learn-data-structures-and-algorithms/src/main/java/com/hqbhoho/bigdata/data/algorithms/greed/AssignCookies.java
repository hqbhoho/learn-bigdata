package com.hqbhoho.bigdata.data.algorithms.greed;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/14
 */
public class AssignCookies {
    public static void main(String[] args) {
        int[] g = new int[]{1, 2};
        int[] s = new int[]{1, 2, 3};
        System.out.println(greedCalc(g, s));

    }

    /**
     * 最小饼干满足最小胃口
     *
     * @param g
     * @param s
     * @return
     */
    public static int greedCalc(int[] g, int[] s) {
        if (g == null || g.length < 1 || s == null || s.length < 1) {
            return 0;
        }
        int res = 0;

        Arrays.sort(g);
        Arrays.sort(s);

        int j = 0;
        for (int i = 0; i < s.length; i++) {
            if (j >= g.length) {
                break;
            }
            if (s[i] >= g[j]) {
                res++;
                j++;
            }
        }
        return res;
    }
}
