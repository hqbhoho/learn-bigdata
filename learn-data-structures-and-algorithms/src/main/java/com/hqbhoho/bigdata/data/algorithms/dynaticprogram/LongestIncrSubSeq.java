package com.hqbhoho.bigdata.data.algorithms.dynaticprogram;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/08
 */
public class LongestIncrSubSeq {
    public static void main(String[] args) {
        int[] nums = new int[]{4,10,4,3,8,9};
        System.out.println(dp(nums));
    }

    /**
     * @param nums
     * @return
     */
    public static int dp(int[] nums) {
        if(nums == null || nums.length == 0) return  0;

        int[] res = new int[nums.length];
        // 状态转移方程
        Arrays.fill(res,1);
        int max = -1;
        for (int i = 0; i < nums.length; i++) {
            for (int j = 0; j < i; j++) {
                if (nums[i] > nums[j]) {
                    res[i] = Math.max(res[i], res[j] + 1);
                }
            }
            max =Math.max(max,res[i]);
        }
        return max;
    }
}
