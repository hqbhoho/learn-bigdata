package com.hqbhoho.bigdata.data.structures.queue;

import java.util.ArrayDeque;
import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/17
 */
public class SlidingWindowMaximum {
    public static void main(String[] args) {
        int[] nums = new int[]{5, 3, 4};
        int[] res = dequeCalcSlidingWindowMaximum(nums, 1);
        Arrays.stream(res).forEach(System.out::println);

    }

    /**
     * 借助一个双端队列完成
     *
     * @param nums
     * @param k
     * @return
     */
    public static int[] dequeCalcSlidingWindowMaximum(int[] nums, int k) {
        ArrayDeque<Integer> arrayDeque = new ArrayDeque();
        int[] res = new int[nums.length - k + 1];
        for (int i = 0; i < nums.length; i++) {
            // 下标超界移除   i=5 时 i<=2的元素不应该出现在队列里面
            Integer first = arrayDeque.peekFirst();
            if (first != null && i - first >= k) {
                arrayDeque.removeFirst();
            }

            // 如果要加入的元素   比已经存在队列中的元素要大   说明已经存在的元素   对后续求最大值没有任何作用   全部弹出
            Integer last = arrayDeque.peekLast();
            while (last != null && nums[last] < nums[i]) {
                arrayDeque.removeLast();
                last = arrayDeque.peekLast();
            }
            arrayDeque.addLast(i);

            // k-1处就应该有结果值了
            if (i >= k - 1) {
                res[i - k + 1] = nums[arrayDeque.getFirst()];
            }
        }

        return res;
    }

    /**
     * 暴力遍历
     *
     * @param nums
     * @param k
     * @return
     */
    public static int[] forceCalcSlidingWindowMaximum(int[] nums, int k) {
        int[] res = new int[nums.length - k + 1];
        int i = 0;
        int j = k - 1;
        for (; j < nums.length; i++, j++) {
            res[i] = getMax(nums, i, j);
        }
        return res;
    }

    public static int getMax(int[] nums, int start, int end) {
        int max = Integer.MIN_VALUE;
        for (int i = start; i <= end; i++) {
            max = Math.max(max, nums[i]);
        }
        return max;
    }

}
