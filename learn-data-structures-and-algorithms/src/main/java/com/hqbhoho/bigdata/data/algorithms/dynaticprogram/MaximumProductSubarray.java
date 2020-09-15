package com.hqbhoho.bigdata.data.algorithms.dynaticprogram;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/07
 */
public class MaximumProductSubarray {
    public static void main(String[] args) {
        int[] nums = new int[]{-3, -4};
        System.out.println(dp(nums));
    }

    public static int dp(int[] nums) {
        // 状态转义方程
        // product[i] = Math(product[i-1] * nums[i],nums[i])

        if (nums == null) return 0;

        int size = nums.length;
        int[] max = new int[size];
        int[] min = new int[size];
        int res = nums[0];
        max[0] = nums[0];
        min[0] = nums[0];
        for (int i = 1; i < nums.length; i++) {
            int value = nums[i];
            // 计算第i位的最大值  -min * vaule(-)  会负负得正  出现最大值
            max[i] = Math.max(min[i - 1] * value, Math.max(value * max[i - 1], value));
            min[i] = Math.min(min[i - 1] * value, Math.min(value * max[i - 1], value));
            res = Math.max(res, max[i]);
        }
        return res;
    }
}
