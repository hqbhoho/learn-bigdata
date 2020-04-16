package com.hqbhoho.bigdata.data.structures.array;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/15
 */
public class RotateArray {
    public static void main(String[] args) {
        int[] array = new int[]{1, 2, 3, 4, 5, 6, 7};
//        forceRotateArray(array, 3);
        reserveRotateArray(array, 2);
        Arrays.stream(array).forEach(System.out::println);


    }

    /**
     * 3次反转
     *
     * @param array
     * @param num
     */
    public static void reserveRotateArray(int[] array, int num) {
        if(array.length <2) return ;
        int move = num % array.length;
        reserveArray(array, 0, array.length - 1);
        reserveArray(array, 0, move - 1);
        reserveArray(array, move, array.length - 1);
    }

    /**
     * 反转数组
     *
     * @param array
     * @param start
     * @param end
     */
    public static void reserveArray(int[] array, int start, int end) {
        int temp = 0;
        for (; start < end; start++, end--) {
            temp = array[start];
            array[start] = array[end];
            array[end] = temp;
        }
    }

    /**
     * 遍历   暴力求解
     * <p>
     * 时间复杂度  O(k*n)
     *
     * @param array
     */
    public static void forceRotateArray(int[] array, int move) {
        for (int i = 1; i <= move % array.length; i++) {
            int temp = array[array.length - 1];
            for (int j = array.length - 1; j > 0; j--) {
                array[j] = array[j - 1];
            }
            array[0] = temp;
        }
    }
}
