package com.hqbhoho.bigdata.data.algorithms.backtracking;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/29
 */
public class Subsets {

    private static List<List<Integer>> res = new ArrayList<>();

    public static void main(String[] args) {
        int[] nums = new int[]{1,2,3};
        recuGenSubsets(nums, new ArrayList<>(), 0);
        System.out.println(res);

        System.out.println(genSubsets(nums));

    }

    /**
     *
     * 非回溯写法
     *
     * @param nums
     * @return
     */
    public static List<List<Integer>> genSubsets(int[] nums) {

        List<List<Integer>> res = new ArrayList<>();
        res.add(new ArrayList<>());

        for (int i = 0; i < nums.length; i++) {
            List<List<Integer>> res1 = new ArrayList<>();
            for(List<Integer> list : res){
                List<Integer> temp = new ArrayList<>(list);
                temp.add(nums[i]);
                res1.add(temp);
            }
            res.addAll(res1);
        }
        return res;
    }

    /**
     * 回溯处理  一般的处理逻辑就是  先添加   递归调用     后移除
     *
     * @param nums
     * @param temp
     * @param level
     */
    public static void recuGenSubsets(int[] nums, List<Integer> temp, int level) {

        //  终止条件
        if (temp.size() > nums.length) {
            return;
        }
        //  处理当前层
        res.add(new ArrayList<>(temp));
        //  进入下一层
        for (int i = level; i < nums.length; i++) {
            temp.add(nums[i]);
            recuGenSubsets(nums, temp, i + 1);
            temp.remove(temp.size() - 1);
        }

    }
}
