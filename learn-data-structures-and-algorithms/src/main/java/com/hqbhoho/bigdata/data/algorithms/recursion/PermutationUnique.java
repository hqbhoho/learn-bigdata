package com.hqbhoho.bigdata.data.algorithms.recursion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/27
 */
public class PermutationUnique {
    private List<List<Integer>> res = new ArrayList<>();

    public static void main(String[] args) {
        PermutationUnique Permutations = new PermutationUnique();
        ArrayList<Integer> temp = new ArrayList<>();
        ArrayList<Integer> visited = new ArrayList<>();
        int[] nums = new int[]{1, 1, 1, 2};
        Arrays.sort(nums);
        Permutations.recuCalc(nums, temp, visited);
        System.out.println(Permutations.res);

    }

    /**
     * 回溯实现
     *
     * @param nums
     * @param temp
     * @param visited
     */
    public void recuCalc(int[] nums, ArrayList<Integer> temp, ArrayList<Integer> visited) {
        // 递归终止条件

        int size = temp.size();
        if (size == nums.length) {
            this.res.add(new ArrayList<>(temp));
            return;
        }

        // 处理当前层

        for (int i = 0; i < nums.length; i++) {

            // 剪枝条件    不太明白
            if (i > 0 && nums[i] == nums[i - 1] && !visited.contains(i - 1)) {
                continue;
            }

            if (!visited.contains(i)) {
                temp.add(nums[i]);
                visited.add(i);
                recuCalc(nums, temp, visited);
                temp.remove(size);
                visited.remove(visited.size() - 1);
            }
        }


        // 递归下一层


        // 移除当前层状态

    }

}
