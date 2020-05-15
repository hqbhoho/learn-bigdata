package com.hqbhoho.bigdata.data.algorithms.binarysearch;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/15
 */
public class ValidPerfectSquare {
    public static void main(String[] args) {
        System.out.println(vaildPerfectSquare(2147483647));
    }

    /**
     * 二分查找
     *
     * @param num
     * @return
     */
    public static boolean vaildPerfectSquare(int num) {
        if (num <= 1) return true;

        int left = 0;
        int right = num;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            long temp = (long)mid * (long)mid;
            System.out.println(mid);
            if (temp == num) {
                return true;
            } else if (temp > num) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return false;
    }
}
