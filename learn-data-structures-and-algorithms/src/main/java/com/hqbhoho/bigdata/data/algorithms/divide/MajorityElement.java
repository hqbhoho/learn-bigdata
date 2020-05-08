package com.hqbhoho.bigdata.data.algorithms.divide;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/29
 */
public class MajorityElement {
    public static void main(String[] args) {
        int[] nums = new int[]{1, 1, 2, 2, 2};
        System.out.println(divideMajorityElement(nums,0,nums.length-1));
    }

    /**
     * 父区间的众数   取决于左右子区间的众数
     * <p>
     * 左右子区间的众数相同   直接就是父区间的众数
     * 左右子区间的众数不同   则需要比较两个众数在父区间   说更加众    作为父区间的众数
     *
     * @param nums
     * @return
     */
    public static int divideMajorityElement(int[] nums, int start, int end) {
        // 分治最底层
        if (start == end) {
            return nums[start];
        }
        // 如何分治
        int mid = (start + end)/2;
        int left = divideMajorityElement(nums,start,mid);
        int right = divideMajorityElement(nums,mid+1,end);
        if(left == right){
            return left;
        }else{
            int leftCount = countByRange(nums,start,end,left);
            int rightCount = countByRange(nums,start,end,right);
            return leftCount >= rightCount ? left : right;
        }
    }

    public static int countByRange(int[] nums, int start, int end,int value){
        int count = 0 ;
        for(int i = start;i<=end;i++){
            if(value == nums[i]){
                count++;
            }
        }
        return count;
    }

    /**
     * 默认是有多数值
     * 排序计算
     *
     * @param nums
     * @return
     */
    public static int calcMajorityElement(int[] nums) {
        Arrays.sort(nums);
        int mid = nums[nums.length / 2];
        return mid;
        /*int i = 0;
        int j = nums.length - 1;

        for (; i < j; ) {
            if (nums[i] != mid) {
                i++;
            }
            if (nums[j] != mid) {
                j--;
            }
            if (nums[i] == mid && nums[j] == mid) {
                break;
            }
        }
        return mid;*/
    }
}
