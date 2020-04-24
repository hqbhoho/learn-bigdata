package com.hqbhoho.bigdata.data.structures.hashtable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/20
 */
public class GroupAnagrams {
    public static void main(String[] args) {
        String[] strings = {"eat", "tea", "tan", "ate", "nat", "bat"};
        System.out.println(calcGroupAnagramsBySortWithHashMap(strings));


    }

    /**
     * 将字符串排序，当作hashmap的key进行统计
     *
     * @param strs
     * @return
     */
    public static List<List<String>> calcGroupAnagramsBySortWithHashMap(String[] strs) {
        if (strs == null || strs.length == 0) return null;
        List<List<String>> result = new ArrayList<>();
        HashMap<String, List<String>> map = new HashMap<>();
        for (int i = 0; i < strs.length; i++) {

            char[] chars = strs[i].toCharArray();
            Arrays.sort(chars);
            String orderStr = new String(chars);
            if (!map.containsKey(orderStr)) {
                ArrayList<String> strings = new ArrayList<>();
                strings.add(strs[i]);
                map.put(orderStr, strings);
            } else {
                map.get(orderStr).add(strs[i]);
            }
        }

        for (List<String> value : map.values()) {
            result.add(value);
        }
        return result;
    }

}
