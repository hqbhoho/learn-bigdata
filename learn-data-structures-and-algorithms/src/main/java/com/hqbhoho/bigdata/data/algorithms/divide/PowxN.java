package com.hqbhoho.bigdata.data.algorithms.divide;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/29
 */
public class PowxN {
    public static void main(String[] args) {
        System.out.println(calcPowxN(2,-2));
    }

    /**
     * 考虑 n 是  正负数
     *
     * @return
     */
    public static double calcPowxN(double x, int n) {
        if (n >= 0) {
            return recuCalcPowxN(x, n);
        } else {
            return 1 / recuCalcPowxN(x, -n);
        }

    }

    /**
     * @param x
     * @param n
     * @return
     */
    public static double recuCalcPowxN(double x, int n) {
        if (n == 1) {
            return x;
        }
        if (n == 0) {
            return 1;
        }

        if (n % 2 == 0) {
            double temp = recuCalcPowxN(x, n / 2);
            return temp * temp;
        } else {
            double temp = recuCalcPowxN(x, n / 2);
            return temp * temp * x;
        }
    }
}
