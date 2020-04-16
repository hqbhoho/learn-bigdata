package com.hqbhoho.bigdata.data.structures.array;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/12
 */
public class ContainerWithMostWater {
    public static void main(String[] args) {
        int[] heights = new int[]{1, 8, 6, 2, 5, 4, 8, 3, 7};
        int maxArea = 0;

//        maxArea = forceCalcMaxArea(heights);
        maxArea = towPointsMoveCalcMaxArea(heights);

        System.out.println(maxArea);
    }


    /**
     * 通过双指针遍历数组，计算最大面值
     * 时间复杂度：O(n)
     * 空间复杂度：O(1)
     *
     * @param heights 每个下标下对应的高度列表
     * @return
     */
    public static int towPointsMoveCalcMaxArea(int[] heights) {
        int maxArea = 0;
        int leftIndex = 0;
        int rightIndex = heights.length - 1;

        while (leftIndex < rightIndex) {
            int width = rightIndex - leftIndex;
            int minHeight = 0;
            if (heights[leftIndex] > heights[rightIndex]) {
                minHeight = heights[rightIndex];
                rightIndex--;
            } else {
                minHeight = heights[leftIndex];
                leftIndex++;
            }
            int area = width * minHeight;
            maxArea = area > maxArea ? area : maxArea;
        }
        return maxArea;
    }


    /**
     * 枚举遍历  暴力求解
     * 时间复杂度：O(n^2)
     * 空间复杂度：O(1)
     *
     * @param heights 每个下标下对应的高度列表
     */
    public static int forceCalcMaxArea(int[] heights) {
        int maxArea = 0;
        for (int i = 0; i <= heights.length - 2; i++) {
            for (int j = i + 1; j <= heights.length - 1; j++) {
                int width = j - i;
                int minHieght = heights[i] > heights[j] ? heights[j] : heights[i];
                int area = width * minHieght;
                maxArea = area > maxArea ? area : maxArea;
            }
        }
        return maxArea;
    }
}



