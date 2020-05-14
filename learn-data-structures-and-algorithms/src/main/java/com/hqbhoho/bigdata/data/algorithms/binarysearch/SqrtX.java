package com.hqbhoho.bigdata.data.algorithms.binarysearch;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/14
 */
public class SqrtX {
    public static void main(String[] args) {

        System.out.println(calc(2147395599));

    }


    /**
     * @param x
     * @return
     */
    public static int calc(int x) {
        int left = 0;
        int right = x;
        int res=-1;
        while (left <= right) {

            int mid = left + (right - left) / 2 ;
            long temp = (long)mid * (long)mid;
            if (temp <= x) {
                res = mid;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return res;
    }

}
