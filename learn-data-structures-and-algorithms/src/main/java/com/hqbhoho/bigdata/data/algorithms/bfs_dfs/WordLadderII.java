package com.hqbhoho.bigdata.data.algorithms.bfs_dfs;

import com.google.common.base.Stopwatch;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/30
 */
public class WordLadderII {
    public static void main(String[] args) {


        Stopwatch watch = Stopwatch.createStarted();
        String beginWord = "qa";
        String endWord = "sq";
        List<String> list = Arrays.asList("si", "go", "se", "cm", "so", "ph", "mt", "db", "mb", "sb", "kr", "ln", "tm", "le", "av", "sm", "ar", "ci", "ca", "br", "ti", "ba", "to", "ra", "fa", "yo", "ow", "sn", "ya", "cr", "po", "fe", "ho", "ma", "re", "or", "rn", "au", "ur", "rh", "sr", "tc", "lt", "lo", "as", "fr", "nb", "yb", "if", "pb", "ge", "th", "pm", "rb", "sh", "co", "ga", "li", "ha", "hz", "no", "bi", "di", "hi", "qa", "pi", "os", "uh", "wm", "an", "me", "mo", "na", "la", "st", "er", "sc", "ne", "mn", "mi", "am", "ex", "pt", "io", "be", "fm", "ta", "tb", "ni", "mr", "pa", "he", "lr", "sq", "ye");
        System.out.println(calcBFS(beginWord, endWord, list));

        System.out.println("运行时间: " + watch.elapsed(TimeUnit.SECONDS) + "s.");
    }


    /**
     * 使用BFS遍历  层次遍历   queue中存放路径List
     *
     * @param beginWord
     * @param endWord
     * @param wordList
     * @return
     */
    public static List<List<String>> calcBFS(String beginWord, String endWord, List<String> wordList) {
        List<List<String>> res = new ArrayList<>();
        if (!wordList.contains(endWord)) {
            return res;
        }

        // 构建BFS遍历需要的队列
        Queue<List<String>> queue = new LinkedList<>();
        ArrayList<String> path = new ArrayList<>(Arrays.asList(beginWord));
        queue.add(path);
        // 词典集合
        HashSet<String> wordSet = new HashSet<>(wordList);
        // 全局访问过的数据集合
        HashSet<String> visited = new HashSet<>();
        // 用于标识在某一层已经出现了目标单词，此时已经识别到了最小的层级，直接跳出循环，不需要在想下一个层级遍历
        boolean flag = true;

        while (!queue.isEmpty() && flag) {
            // 某一层级的path的数量
            int size = queue.size();
            // 每个层级访问数据的集合
            HashSet<String> subVisited = new HashSet<>();
            for (int j = 0; j < size; j++) {


                List<String> tempPath = queue.poll();
                String tempword = tempPath.get(tempPath.size() - 1);

                // 当前层级已经到达了目的单词，修改状态位并将结果加入到最终的结果中
                if(tempword.equalsIgnoreCase(endWord)){
                    flag = false;
                    res.add(new ArrayList<>(tempPath));
                }

                for (int i = 0; i < tempword.length(); i++) {
                    for (char ch = 'a'; ch <= 'z'; ch++) {

                        char[] chars = tempword.toCharArray();
                        if (chars[i] == ch) {
                            continue;
                        }
                        chars[i] = ch;
                        String newCur = new String(chars);
                        if (!visited.contains(newCur) && wordSet.contains(newCur)) {
                            List<String> newPath = new ArrayList<>(tempPath);
                            newPath.add(newCur);
                            queue.add(newPath);
                            subVisited.add(newCur);
                        }
                    }
                }
            }
            visited.addAll(subVisited);
        }
        return res;
    }


    //  下面的方法会超时，遍历出来了所有的情况，大部分并不是最短路径

    /**
     * @param beginWord
     * @param endWord
     * @param wordList
     * @return
     */
    public static List<List<String>> calcDFS(String beginWord, String endWord, List<String> wordList) {

        Stack<List<String>> res = new Stack<>();
        if (!wordList.contains(endWord)) {
            return res;
        }


        LinkedList<String> queue = new LinkedList<>();
        Set<String> wordSet = new HashSet<>(wordList);
        Set<String> visited = new HashSet<>();
        visited.add(beginWord);
        queue.addFirst(beginWord);
        List<String> temp = new ArrayList<>();
        temp.add(beginWord);

        DFS(beginWord, endWord, wordSet, visited, temp, res);
        System.out.println(res);
        return res;

    }

    /**
     * 深度优先遍历
     *
     * @param beginWord
     * @param endWord
     * @param wordSet
     * @param visited
     * @param temp
     * @param res
     */
    private static void DFS(String beginWord, String endWord,
                            Set<String> wordSet, Set<String> visited, List<String> temp, Stack<List<String>> res) {
        if (beginWord.equalsIgnoreCase(endWord)) {
            List<String> temp1 = new ArrayList<>(temp);
            int size = temp1.size();
            if (res.empty() || (!res.empty() && res.peek().size() == size)) {
                res.push(temp1);
                return;
            }
            if (res.empty() || (!res.empty() && res.peek().size() < size)) {
                return;
            }

            while (!res.empty() && res.peek().size() > size) {
                res.pop();
            }
            res.push(temp1);
            return;
        }

        for (int i = 0; i < beginWord.length(); i++) {
            for (char ch = 'a'; ch <= 'z'; ch++) {

                char[] chars = beginWord.toCharArray();
                if (chars[i] == ch) {
                    continue;
                }
                chars[i] = ch;
                String newCur = new String(chars);
                if (!visited.contains(newCur) && wordSet.contains(newCur)) {
                    visited.add(newCur);
                    temp.add(newCur);
                    DFS(newCur, endWord, wordSet, visited, temp, res);
                    temp.remove(temp.size() - 1);
                    visited.remove(newCur);
                }
            }
        }

    }
}
