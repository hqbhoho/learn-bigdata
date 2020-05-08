package com.hqbhoho.bigdata.data.algorithms.bfs_dfs;

import java.util.*;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/30
 */
public class WordLadder {
    public static void main(String[] args) {
        String beginWord = "a";
        String endWord = "c";
        List<String> wordList = Arrays.asList("a", "b", "c");
        System.out.println(doubleBFS(beginWord, endWord, wordList));


    }

    /**
     * 双向BFS
     *
     * @param beginWord
     * @param endWord
     * @param wordList
     * @return
     */
    public static int doubleBFS(String beginWord, String endWord, List<String> wordList) {
        if (!wordList.contains(endWord)) {
            return 0;
        }

        LinkedList<String> startQueue = new LinkedList<>();
        LinkedList<String> endQueue = new LinkedList<>();
        Set<String> wordSet = new HashSet<>(wordList);
        Set<String> startVisited = new HashSet<>();
        Set<String> endVisited = new HashSet<>();
        startVisited.add(beginWord);
        endVisited.add(endWord);
        startQueue.addFirst(beginWord);
        endQueue.addFirst(endWord);


        int res = 1;
        while (!startQueue.isEmpty() && !endQueue.isEmpty()) {

            // 每次从较小的队列中开始遍历
            if (startQueue.size() > endQueue.size()) {
                LinkedList<String> tempQueue = startQueue;
                startQueue = endQueue;
                endQueue = tempQueue;
                Set<String> tempVisited = startVisited;
                startVisited = endVisited;
                endVisited = tempVisited;
            }

            int count = startQueue.size();
            while (count > 0) {
                String cur = startQueue.removeLast();
                if (endVisited.contains(cur)) {
                    return res;
                }
                for (char ch = 'a'; ch <= 'z'; ch++) {
                    for (int i = 0; i < beginWord.length(); i++) {
                        char[] chars = cur.toCharArray();
                        if (chars[i] == ch) {
                            continue;
                        }
                        chars[i] = ch;
                        String newCur = new String(chars);
                        if (!startVisited.contains(newCur) && wordSet.contains(newCur)) {

                            startVisited.add(newCur);
                            startQueue.addFirst(newCur);
                        }
                    }
                }
                count--;
            }
            res++;
        }
        return 0;
    }


    /**
     * 广度优先遍历
     *
     * @param beginWord
     * @param endWord
     * @param wordList
     * @return
     */
    public static int BFS(String beginWord, String endWord, List<String> wordList) {

        if (!wordList.contains(endWord)) {
            return 0;
        }

        LinkedList<String> queue = new LinkedList<>();
        Set<String> wordSet = arrayToSet(wordList);
        Set<String> visited = new HashSet<>();
        visited.add(beginWord);
        queue.addFirst(beginWord);

        int res = 1;
        while (!queue.isEmpty()) {
            int count = queue.size();
            while (count > 0) {
                String cur = queue.removeLast();
                if (cur.equalsIgnoreCase(endWord)) {
                    return res;
                }
                for (char ch = 'a'; ch <= 'z'; ch++) {
                    for (int i = 0; i < beginWord.length(); i++) {
                        char[] chars = cur.toCharArray();
                        if (chars[i] == ch) {
                            continue;
                        }
                        chars[i] = ch;
                        String newCur = new String(chars);
                        if (!visited.contains(newCur) && wordSet.contains(newCur)) {
                            if (newCur.equalsIgnoreCase(endWord)) {
                                return res + 1;
                            }
                            visited.add(newCur);
                            queue.addFirst(newCur);
                        }
                    }
                }
                count--;
            }
            res++;
        }
        return 0;
    }

    public static Set<String> arrayToSet(List<String> bank) {

        HashSet set = new HashSet<>();
        for (String str : bank) {
            set.add(str);
        }
        return set;
    }


}
