package com.hqbhoho.bigdata.data.algorithms.dynaticprogram;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/07
 */
public class MaxiumSubarray {
    public static void main(String[] args) {
        int[] nums = new int[]{-2,1,-3,4,-1,2,1,-5,4};
        System.out.println(dp(nums));
    }

    public static int dp(int[] nums){
        // 状态转移方程   第i位  算不算前面的  连续子序列
        // sum[i] = max(sum[i-1] + num[i], num[i])
        if(nums == null )return 0;

        int max = nums[0];
        for(int i = 1 ;i< nums.length ;i++){
            int value = nums[i];
            // 计算第i位的最大值
            nums[i] = Math.max(nums[i-1]+value,value);
            max = Math.max(nums[i],max);
        }
        return max;


    }
}
