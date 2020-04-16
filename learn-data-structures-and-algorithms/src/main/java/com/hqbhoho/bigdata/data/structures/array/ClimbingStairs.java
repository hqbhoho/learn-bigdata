package com.hqbhoho.bigdata.data.structures.array;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/12
 */
public class ClimbingStairs {
    public static void main(String[] args) {
//        System.out.println(calc1(4));
        System.out.println(calc1(4));
    }

    /**
     * 处理逻辑是一样的，只是只用3个变量而非数组来记录数据
     *
     * @param n
     * @return
     */
    public static int calc2(int n) {
        if (n == 1) {
            return 1;
        }
        if (n == 2) {
            return 2;
        }

        int x = 1;
        int y = 2;
        int z = 0;
        for (int i = 3; i <= n; i++) {
            z = x + y;
            x = y;
            y = z;
        }
        return z;
    }


    /**
     * 一次上一步或者上两步
     * 到达第n阶 要不从n-1阶跨一步   要不从n-2阶跨两步
     * arr[n] = arr[n-1] + arr[n-2]
     *
     * @param n
     * @return
     */
    public static int calc1(int n) {
        if (n == 1) {
            return 1;
        }
        if (n == 2) {
            return 2;
        }
        int[] arr = new int[n + 1];

        arr[1] = 1;
        arr[2] = 2;
        for (int i = 3; i <= n; i++) {
            arr[i] = arr[i - 1] + arr[i - 2];
        }
        return arr[n];
    }
}
