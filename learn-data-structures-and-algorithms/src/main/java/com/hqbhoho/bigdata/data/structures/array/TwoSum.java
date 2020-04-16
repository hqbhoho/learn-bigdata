package com.hqbhoho.bigdata.data.structures.array;

import java.util.Arrays;
import java.util.HashMap;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/16
 */
public class TwoSum {
    public static void main(String[] args) {
        int[] nums = new int[]{2, 7, 11, 15};
        int target = 9;
        Arrays.stream(hashMapcalcTwoSum2(nums, 9)).forEach(System.out::println);


    }

    /**
     * 基于外部一个哈希表实现
     * 遍历一次   边遍历边插入数据
     *
     * @param nums
     * @param target
     * @return
     */
    public static int[] hashMapcalcTwoSum2(int[] nums, int target) {
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length ; i++) {
            Integer j = map.get(target - nums[i]);
            if (j != null) {
                return new int[]{i, j};
            }
            map.put(nums[i], i);
        }
        return null;
    }

    /**
     * 基于外部一个哈希表实现
     * 遍历两次
     *
     * @param nums
     * @param target
     * @return
     */
    public static int[] hashMapcalcTwoSum(int[] nums, int target) {
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length ; i++) {
            map.put(nums[i], i);
        }
        for (int i = 0; i < nums.length ; i++) {
            Integer j = map.get(target - nums[i]);
            if (j != null && j != i) {
                return new int[]{i, j};
            }
        }
        return null;
    }
}
