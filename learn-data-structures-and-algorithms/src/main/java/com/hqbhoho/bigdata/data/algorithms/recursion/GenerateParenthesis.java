package com.hqbhoho.bigdata.data.algorithms.recursion;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/22
 */
public class GenerateParenthesis {
    public static void main(String[] args) {
        ArrayList<String> res = new ArrayList<>();
        recuGenerateParenthesis(3,0,0,"",res);
        System.out.println(res);

    }
    public static void recuGenerateParenthesis(int n,int left,int right,String temp,List<String> res){
        // 递归终止条件
        if(left == n && right == n){
            res.add(temp);
            return ;
        }

        // 当前层处理逻辑

        // 向下一层递归
        if(left < n){

            String temp1 = temp +"(";

            recuGenerateParenthesis(n,left+1,right,temp1 ,res);
        }
        if(right < left){

            String temp2 = temp +")";
            recuGenerateParenthesis(n,left,right+1,temp2,res);
        }

        // 处理扫尾
    }
}
