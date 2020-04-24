package com.hqbhoho.bigdata.data.structures.array;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/18
 */
public class TrappingRainWater {
    public static void main(String[] args) {
        int[] array = new int[]{0, 1, 0, 2, 1, 0, 1, 3, 2, 1, 2, 1};
        System.out.println(forceCalc(array));

    }


    /**
     * 暴力遍历
     * <p>
     * 找到当前节点的左右的最高点
     *
     * @param array
     * @return
     */
    public static int forceCalc(int[] array) {
        int res = 0;
        for (int i = 0; i < array.length; i++) {
            int leftHight = array[i];
            int rightHight = array[i];
            for (int j = i - 1; j >= 0; j--) {
                leftHight = Math.max(array[j], leftHight);
            }
            for (int k = i + 1; k < array.length; k++) {
                rightHight = Math.max(array[k], rightHight);
            }
            res += Math.min(leftHight, rightHight) - array[i];
        }
        return res;
    }
}
