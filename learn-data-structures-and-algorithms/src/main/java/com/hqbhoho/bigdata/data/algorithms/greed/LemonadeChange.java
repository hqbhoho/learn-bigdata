package com.hqbhoho.bigdata.data.algorithms.greed;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/08
 */
public class LemonadeChange {
    public static void main(String[] args) {
        int[] bills = new int[]{10,20};
        System.out.println(greedCalc(bills));

    }


    /**
     *
     * 贪心算法，优先使用$10找零
     *
     * @param bills
     * @return
     */
    public static boolean greedCalc(int[] bills){
        int five = 0;
        int ten = 0;
        for(int i= 0;i< bills.length;i++){

            if(bills[i] == 5){ // 收到是$5
                five++;
            }else if(bills[i] == 10){ // 收到是$10
                five--;
                ten++;
            }else{   // 收到是$20
                if(ten > 0 ){
                    ten--;
                }else{
                    five--;
                    five--;
                }
                five--;
            }
            if(five < 0 || ten < 0){
                return false;
            }

        }
        return true;
    }
}
