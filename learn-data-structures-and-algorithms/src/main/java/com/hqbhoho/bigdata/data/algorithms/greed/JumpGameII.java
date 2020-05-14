package com.hqbhoho.bigdata.data.algorithms.greed;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/14
 */
public class JumpGameII {
    public static void main(String[] args) {
        int[] nums = new int[]{2, 3, 1, 1, 4};
        System.out.println(greedCalc(nums));

    }

    /**
     * 贪心算法每次找最优解  跳到最远
     *
     * @param nums
     * @return
     */
    public static int greedCalc(int[] nums) {
        int res = 0;
        // 当前index 到 当前index所能到达的最远边界之间遍历数据   能生成的最远的下一次的边界
        int maxPos = 0;
        // 当前index  所能到达的最远边界
        int nextIndex = 0;
        for (int i = 0; i < nums.length - 1; i++) {
            maxPos = Math.max(maxPos, nums[i] + i);

            if (i == nextIndex) {
                nextIndex = maxPos;
                res++;
            }
        }
        return res;
    }
}
