package com.hqbhoho.bigdata.data.structures.array;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/16
 */
public class MergeSortedArray {
    public static void main(String[] args) {
        int[] nums1 = new int[]{1, 2, 3, 0, 0, 0};
        int[] nums2 = new int[]{2, 5, 6};
        twoPointMergeSortArray(nums1, 3, nums2, 3);
        Arrays.stream(nums1).forEach(System.out::println);

    }

    /**
     * 双指针
     *
     * @param nums1
     * @param m
     * @param nums2
     * @param n
     */
    public static void twoPointMergeSortArray(int[] nums1, int m, int[] nums2, int n) {

        int cur1 = m - 1;
        int cur2 = n - 1;
        int i = 0;

        for ( ;cur1 >= 0 && cur2 >= 0; i++) {
            if (nums1[cur1] > nums2[cur2]) {
                nums1[m + n - 1 - i] = nums1[cur1];
                cur1--;
            } else {
                nums1[m + n - 1 - i] = nums2[cur2];
                cur2--;
            }
        }

        // 前一步中如果nums中还有元素  则要移动到nums1中
        for(;cur2>=0;cur2--){
            nums1[m + n - 1 - i] = nums2[cur2];
            i++;
        }

    }

    /**
     * 借助一个外部数组实现
     *
     * @param nums1
     * @param m
     * @param nums2
     * @param n
     */
    public static void extraMergeSortedArray(int[] nums1, int m, int[] nums2, int n) {
        int[] temp = new int[m + n];
        int i = 0, j = 0;
        int index = 0;
        for (; i < m && j < n; ) {
            if (nums1[i] < nums2[j]) {
                temp[index] = nums1[i];
                i++;
            } else {
                temp[index] = nums2[j];
                j++;
            }
            index++;
        }
        if (j == n) {
            for (; index < m + n; index++) {
                temp[index] = nums1[i];
                i++;
            }
        } else {
            for (; index < m + n; index++) {
                temp[index] = nums2[j];
                j++;
            }
        }
        for (int k = 0; k < m + n; k++) {
            nums1[k] = temp[k];
        }
    }

    /**
     * @param nums1
     * @param m
     * @param nums2
     * @param n
     */
    public void forceMergeSortedArray(int[] nums1, int m, int[] nums2, int n) {
        for (int i = 1; i < n; i++) {
            nums1[m + i] = nums2[i];
        }
        Arrays.sort(nums1);
    }
}
