package com.hqbhoho.bigdata.data.algorithms.backtracking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/29
 */
public class LetterCombinationsPhoneNumber {

    private List<String> res = new ArrayList<>();

    public static void main(String[] args) {
        Map<Character, String> phone = new HashMap<>();
        phone.put('2', "abc");
        phone.put('3', "def");
        phone.put('4', "ghi");
        phone.put('5', "jkl");
        phone.put('6', "mno");
        phone.put('7', "pqrs");
        phone.put('8', "tuv");
        phone.put('9', "wxyz");
        LetterCombinationsPhoneNumber letterCombinationsPhoneNumber = new LetterCombinationsPhoneNumber();
        letterCombinationsPhoneNumber.calcLetterCombinationsPhoneNumber(phone, "23", 0, new StringBuilder());
        System.out.println(letterCombinationsPhoneNumber.res);

    }

    /**
     * 回溯
     *
     * @param phone
     * @param digits
     * @param level
     * @param temp
     */
    public void calcLetterCombinationsPhoneNumber(Map<Character, String> phone, String digits, int level, StringBuilder temp) {
        if(digits.length() == 0){
            return;
        }

        // 终止条件
        if (digits.length() < level + 1) {
            res.add(temp.toString());
            return;
        }
        // 处理当前逻辑
        String s = phone.get(digits.charAt(level));

        // 进入下一层
        for (int i = 0; i < s.length(); i++) {
            temp.append(s.charAt(i));
            calcLetterCombinationsPhoneNumber(phone, digits, level + 1, temp);
            temp.deleteCharAt(temp.length() - 1);
        }


    }
}
