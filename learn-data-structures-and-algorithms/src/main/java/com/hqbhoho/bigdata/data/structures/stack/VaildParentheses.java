package com.hqbhoho.bigdata.data.structures.stack;

import java.util.HashMap;
import java.util.Stack;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/17
 */
public class VaildParentheses {
    public static void main(String[] args) {

        String str = "(";
        System.out.println(calcVaildParentheses(str));
    }

    /**
     * 借助一个栈来实现
     *
     * @param str
     * @return
     */
    public static boolean calcVaildParentheses(String str) {

        Stack<Character> chars = new Stack<>();
        HashMap<Character, Character> map = new HashMap<>();
        map.put('[', ']');
        map.put('{', '}');
        map.put('(', ')');

        for (int i = 0; i < str.length(); i++) {
            Character temp = str.charAt(i);
            if (map.containsKey(temp)) {
                chars.add(temp);
            } else {
                if (chars.isEmpty()) return false;
                Character first = chars.peek();
                if (map.get(first) == temp) {
                    chars.pop();
                } else {
                    return false;
                }
            }
        }
        return chars.isEmpty();
    }
}
