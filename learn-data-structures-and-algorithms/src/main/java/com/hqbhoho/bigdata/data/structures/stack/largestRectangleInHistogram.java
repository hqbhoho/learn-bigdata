package com.hqbhoho.bigdata.data.structures.stack;

import java.util.Stack;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/17
 */
public class largestRectangleInHistogram {
    public static void main(String[] args) {
        int[] heights = new int[]{1, 2, 3, 4, 5};
        System.out.println(stackClac(heights));

    }

    /**
     * 借助栈来进行计算   堆里面存储数组的角标
     * <p>
     * 通过栈来找到每个高度的左右边界  进而计算最大面积
     * 栈中只存放一个递增的序列
     *
     * @param heights
     * @return
     */
    public static int stackClac(int[] heights) {
        int maxArea = 0;
        Stack<Integer> heightStack = new Stack<>();
        // 加入一个左边界
        heightStack.add(-1);
        for (int i = 0; i < heights.length; i++) {
            Integer first = heightStack.peek();
            while (first != -1 && heights[first] >= heights[i]) {
                Integer pop = heightStack.pop();
                first = heightStack.peek();
                // 右边界是i  左边界是 前一个节点
                maxArea = Math.max((i - first - 1) * heights[pop], maxArea);
            }
            heightStack.add(i);
        }
        Integer pop = heightStack.pop();
        Integer res = pop;
        // 这个就是一个递增的直方图
        while (pop != -1) {
            Integer first = heightStack.peek();
            maxArea = Math.max(maxArea, (res - first) * heights[pop]);
            pop = heightStack.pop();

        }
        return maxArea;
    }

    /**
     * 暴力遍历
     * 时间复杂度: O(n^3)
     * leetcode 超时
     *
     * @param heights
     * @return
     */
    public static int forceCalc(int[] heights) {
        int maxArea = 0;
        for (int i = 0; i < heights.length; i++) {
            maxArea = Math.max(maxArea, heights[i]);
            for (int j = i + 1; j < heights.length; j++) {
                int width = j - i + 1;
                int minHeight = Integer.MAX_VALUE;
                for (int k = i; k <= j; k++) {
                    minHeight = Math.min(minHeight, heights[k]);
                }
                maxArea = Math.max(maxArea, width * minHeight);
            }
        }
        return maxArea;
    }
}
