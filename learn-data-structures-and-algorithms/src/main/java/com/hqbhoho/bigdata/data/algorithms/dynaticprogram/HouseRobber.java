package com.hqbhoho.bigdata.data.algorithms.dynaticprogram;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/08
 */
public class HouseRobber {
    public static void main(String[] args) {
        int[] nums = new int[]{1, 2, 3, 1};
        System.out.println(dp(nums));

    }

    /**
     * @param nums
     * @return
     */
    public static int dp(int[] nums) {
        // 状态转移方程
        // dp[][]
        // dp[i][0] = math.max(dp[i-1][i],dp[i-1][0] + nums[i])

        if (nums == null || nums.length == 0) {
            return 0;
        }
        if (nums.length == 1) {
            return nums[0];
        }
        if (nums.length == 2) {
            return Math.max(nums[0], nums[1]);
        }
        // 状态定义
        int[] dp = new int[nums.length];

        dp[0] = nums[0];
        dp[1] = Math.max(nums[0], nums[1]);
        for (int i = 2; i < nums.length; i++) {
            // i-1 偷或者不偷
            dp[i] = Math.max(dp[i - 1], dp[i - 2] + nums[i]);

        }

        return dp[nums.length - 1];
    }
}
