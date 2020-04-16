package com.hqbhoho.bigdata.data.structures.array;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/15
 */
public class RemoveDupliFromSortedArray {
    public static void main(String[] args) {
        int[] array = new int[]{0,0,1,1,1,2,2,3,3,4};
        System.out.println(removeDupliFromSortedArray(array));


    }

    /**
     *
     * 双下标移动
     *
     * @return
     */
    public static int removeDupliFromSortedArray(int[] array){
        if(array.length == 0) return 0;
        int j = 0;
        for(int i=0;i< array.length-1;i++){
            if(array[i] == array[i+1]){
                continue;
            }
            array[j++] = array[i];
        }
        array[j++] = array[array.length-1];
        return j;
    }


}
