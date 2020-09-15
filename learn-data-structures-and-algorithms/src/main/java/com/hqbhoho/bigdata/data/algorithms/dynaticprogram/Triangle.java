package com.hqbhoho.bigdata.data.algorithms.dynaticprogram;

import java.util.Arrays;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/15
 */
public class Triangle {
    public static void main(String[] args) {
        List<List<Integer>> triangle = Arrays.asList(
                Arrays.asList(2),
                Arrays.asList(3, 4),
                Arrays.asList(6, 5, 7),
                Arrays.asList(4, 1, 8, 3)

        );
        System.out.println(downDp(triangle));
    }

    /**
     *
     * @param triangle   自底向上
     * @return
     */
    public static int downDp(List<List<Integer>> triangle) {
        int size = triangle.size();
        if(triangle == null || size == 0 ) return 0;
        // 只记录每一层的最小值
        int[] min = new int[size+1];
        for (int i = size - 1; i >= 0; i--) {
            for (int j = 0; j <= i; j++) {
                // 状态转移方程
                min[j] = Math.min(min[j], min[j + 1]) + triangle.get(i).get(j);
            }
        }
        return min[0];

        //   分治 （拆分子问题）

        // f(i,j) = min( f(i-1,j),f(i-1,j+1))

        //   定义状态数组


        //  状态转义方程


    }
}
