package com.hqbhoho.bigdata.data.structures.array;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/12
 */
public class MoveZeroes {
    public static void main(String[] args) {
        int[] arr = new int[]{7, -1, 0, 3, 12,0,0,1};

//        forceMoveZeroes(arr);
//        twoPointsMoveZeroes1(arr);
        twoPointsMoveZeroes2(arr);

        Arrays.stream(arr).forEach(System.out::println);

    }


    /**
     *
     * 此方法   尚待推敲
     *
     * @param nums
     * @return
     */
    public static int[] twoPointsMoveZeroes2(int[] nums) {
            //两个指针i和j
            int j = 0;
            for(int i=0;i<nums.length;i++) {
                //当前元素!=0，就把其交换到左边，等于0的交换到右边

                if(nums[i]!=0) {
                    int tmp = nums[i];
                    nums[i] = nums[j];
                    nums[j++] = tmp;
                }
            }
            return nums;
    }



    /**
     * 双指针移动
     * 时间复杂度 O(n)
     * <p>
     * i 0->array.length-1 遍历  碰到非0数就将其赋值给 array[j] ,当i遍历一遍之后，j指针以后的值就全部是0了
     *
     * @param array
     * @return
     */
    public static int[] twoPointsMoveZeroes1(int[] array) {
        int j = 0;
        for (int i = 0; i < array.length; i++) {
            if (array[i] != 0) {
                array[j] = array[i];
                j++;
            }
        }
        while (j < array.length) {
            array[j] = 0;
            j++;
        }
        return array;
    }


    /**
     * 暴力求解
     * <p>
     * 时间复杂度：O(n^2)
     *
     * @param array
     * @return
     */
    public static int[] forceMoveZeroes(int[] array) {
        for (int i = 0; i < array.length - 1; i++) {
            if (array[i] == 0) {
                for (int j = i + 1; j < array.length; j++) {
                    if (array[j] != 0) {
                        int temp = array[i];
                        array[i] = array[j];
                        array[j] = temp;
                        break;
                    }

                }
            }
        }
        return array;
    }
}
