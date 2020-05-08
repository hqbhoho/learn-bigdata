package com.hqbhoho.bigdata.data.algorithms.bfs_dfs;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/30
 */
public class MinimumGeneticMutation {
    public static void main(String[] args) {
        String start = "AACCGGTT";
        String end = "AACCGGTA";
        String[] bank = new String[]{"AACCGGTA"};
        int i = calcMinimumGeneticMutation(start, end, bank);
        System.out.println(i);
    }

    /**
     * 使用广度优先遍历
     *
     * @param start
     * @param end
     * @param bank
     * @return
     */
    public static int calcMinimumGeneticMutation(String start, String end, String[] bank) {
        char[] chars = new char[]{'A', 'C', 'G', 'T'};
        Set<String> visisted = new HashSet<>();
        Set<String> bankSet = arrayToSet(bank);
        if (!bankSet.contains(end)) {
            return -1;
        }
        LinkedList<String> queue = new LinkedList<>();
        queue.addFirst(start);

        int res = 0;
        while (!queue.isEmpty()) {
            int count = queue.size();
            while (count > 0) {
                String cur = queue.removeLast();
                if (cur.equalsIgnoreCase(end)) {
                    return res;
                }

                for (char ch : chars) {
                    for (int i = 0; i < 8; i++) {
                        char[] chars1 = cur.toCharArray();
                        if(chars1[i] == ch){
                            continue;
                        }
                        chars1[i] = ch;
                        String newCur = new String(chars1);
                        if (!visisted.contains(newCur) && bankSet.contains(newCur)) {
                            if(newCur.equalsIgnoreCase(end)){
                                return res+1;
                            }
                            visisted.add(newCur);
                            queue.addFirst(newCur);

                        }
                    }
                }
                count--;
            }

            if(!queue.isEmpty()) res++;

        }

        return -1;
    }

    public static Set<String> arrayToSet(String[] bank) {

        HashSet set = new HashSet<>();
        for (String str : bank) {
            set.add(str);
        }
        return set;
    }


}
