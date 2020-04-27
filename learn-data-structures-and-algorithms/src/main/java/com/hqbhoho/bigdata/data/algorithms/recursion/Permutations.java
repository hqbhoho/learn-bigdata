package com.hqbhoho.bigdata.data.algorithms.recursion;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/27
 */
public class Permutations {

    private List<List<Integer>> res = new ArrayList<>();

    public static void main(String[] args) {
        Permutations Permutations = new Permutations();
        ArrayList<Integer> temp = new ArrayList<>();
        ArrayList<Integer> visited = new ArrayList<>();
        Permutations.recuCalc(new int[]{1,2,3}, temp, visited);
        System.out.println(Permutations.res);
    }

    /**
     *
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
            if (!visited.contains(i)) {
                temp.add(nums[i]);
                visited.add(i);
                recuCalc(nums, temp, visited);
                temp.remove(size);
                visited.remove(visited.size()-1);
            }
        }


        // 递归下一层


        // 移除当前层状态

    }

}
