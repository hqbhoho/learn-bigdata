package com.hqbhoho.bigdata.data.algorithms.greed;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/14
 */
public class JumpGame {
    public static void main(String[] args) {
        int[] nums = new int[]{2, 3, 1, 1, 4};
        System.out.println(greedCalc(nums));

    }

    /**
     * 倒序往前推，不断更新pos位置，pos的位置一定是能到达最后的
     *
     * @return
     */
    public static boolean greedCalc(int[] nums) {
        if (nums == null || nums.length == 0) {
            return false;
        }

        int pos = nums.length - 1;
        for (int i = nums.length - 2; i >= 0; i--) {
            if (nums[i] + i >= pos) {
                pos = i;
            }
        }
        return pos == 0;
    }
}
