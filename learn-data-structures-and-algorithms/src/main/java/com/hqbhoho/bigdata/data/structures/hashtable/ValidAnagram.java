package com.hqbhoho.bigdata.data.structures.hashtable;

import java.util.HashMap;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/20
 */
public class ValidAnagram {
    public static void main(String[] args) {
        String str1 = "a";
        String str2 = "b";
        System.out.println(calcVaildAnagramWithArray(str1,str2));
    }

    /**
     *
     * 设字符串只包含小写字母   只需要一个长度为26的数组基数即可
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean calcVaildAnagramWithArray(String str1, String str2){
        if (str1 == null || str2 == null ) {
            return false;
        }
        if (str1.length() != str2.length()) {
            return false;
        }
        int[] count = new int[26];
        for (int i =0 ;i<str1.length();i++) {
            Character ch1 = str1.charAt(i);
            count[ch1-'a'] ++;
            Character ch2 = str2.charAt(i);
            count[ch2-'a'] --;
        }

        for(int i =0 ; i< 26;i++ ){
            if(count[i] != 0){
                return false;
            }
        }
        return true;
    }

    /**
     * 借助Hash存储字符出现的次数
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean calcValidAnagramWithHashMap(String str1, String str2) {
        if (str1 == null || str2 == null ) {
            return false;
        }
        if (str1.length() != str2.length()) {
            return false;
        }
        HashMap<Character,Integer> charMap = new HashMap<>();
        for (int i =0 ;i<str1.length();i++) {
            Character ch = str1.charAt(i);
            if(!charMap.containsKey(ch)){
                charMap.put(ch,1);
            }else{
                charMap.put(ch,charMap.get(ch)+1);
            }
        }

        for(int i =0 ;i<str2.length();i++){
            Character ch = str2.charAt(i);
            if(!charMap.containsKey(ch)){
               return false;
            }else if(charMap.get(ch) == 0){
                return false;
            }else{
                charMap.put(ch,charMap.get(ch)-1);
            }
        }
        return true;
    }


}
