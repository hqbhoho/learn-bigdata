package com.hqbhoho.bigdata.data.structures.array;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/16
 */
public class PlusOne {
    public static void main(String[] args) {
        int[] digits = new int[]{9,9,9};
        Arrays.stream(calcPlusOne(digits)).forEach(System.out::println);

    }

    /**
     *
     * @param digits
     * @return
     */
    public static int[] calcPlusOne(int[] digits){
        for(int i=digits.length-1;i>=0;i--){
            if(digits[i] ==9){
                digits[i] = 0;
            }else{
                digits[i] += 1;
                break;
            }
        }
        if(digits[0] == 0){
            int[] res = new int[digits.length+1];
            res[0] = 1;
            return res;
        }
        return digits;
    }
}
