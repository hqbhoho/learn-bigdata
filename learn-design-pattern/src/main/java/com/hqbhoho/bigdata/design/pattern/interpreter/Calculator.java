package com.hqbhoho.bigdata.design.pattern.interpreter;

import java.util.HashMap;
import java.util.Stack;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class Calculator {

    private Expression prase(String calStr){
        char[] chars = calStr.toCharArray();
        Stack<Expression> stack = new Stack<>();
        for(int i = 0 ; i< chars.length ;){
            switch (chars[i]){
                case '+': {
                    Expression left = stack.pop();
                    Expression right = new VarExpression(String.valueOf(chars[++i]));
                    stack.push(new AddExpression(left,right));
                    break;
                }
                case '-':{
                    Expression left = stack.pop();
                    Expression right = new VarExpression(String.valueOf(chars[++i]));
                    stack.push(new SubExpression(left,right));
                    break;
                }
                default:{
                    stack.push(new VarExpression(String.valueOf(chars[i])));
                    i++;
                }
            }
        }
        return  stack.pop();
    }
    public int run(String calStr,HashMap<String,Integer> map){
        return prase(calStr).interpreter(map);
    }
}
