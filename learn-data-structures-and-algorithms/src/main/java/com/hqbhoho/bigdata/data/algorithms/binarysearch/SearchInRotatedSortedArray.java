package com.hqbhoho.bigdata.data.algorithms.binarysearch;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/15
 */
public class SearchInRotatedSortedArray {

    public static void main(String[] args) {
        System.out.println(calcIndex(new int[]{1, 3, 5}, 5));
    }

    /**
     * 旋转数组的特点   0, mid ,nums.length-1  egn'g
     *
     * @param nums
     * @param target
     * @return
     */
    public static int calcIndex(int[] nums, int target) {

        if (nums == null || nums.length == 0) return -1;

        int left = 0;
        int right = nums.length - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (nums[mid] == target) {
                return mid;
            }
            if (nums[0] <= nums[mid]) {
                if (target >= nums[0] && target < nums[mid]) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            } else {
                if (target > nums[mid] && target <= nums[nums.length - 1]) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }

            }


        }
        return -1;
    }
}
