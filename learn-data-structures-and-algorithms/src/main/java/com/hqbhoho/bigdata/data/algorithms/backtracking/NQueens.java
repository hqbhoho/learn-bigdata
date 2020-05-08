package com.hqbhoho.bigdata.data.algorithms.backtracking;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/29
 */
public class NQueens {

    List<List<String>> res = new ArrayList<>();

    public static void main(String[] args) {
        int n = 8;
        NQueens nQueens = new NQueens();
        int[][] visited = new int[n][n];
        nQueens.calcNQueens(n, 0, nQueens.init(n),visited);
        System.out.println(nQueens.res);


    }

    public void calcNQueens(int n, int level, List<String> temp,int[][] visited) {


        if (n <= level) {
            res.add(new ArrayList<>(temp));
            return;
        }
        // 第level层出处理逻辑
        for (int i = 0; i < n; i++) {
            // 首先判断i
            if (invalid(level, i,n,visited)) {
                String s = temp.get(level);
                char[] chars = s.toCharArray();
                chars[i] = 'Q';

                temp.set(level, new String(chars));
                visited[level][i] = 1;
                calcNQueens(n, level + 1, temp,visited);
                temp.set(level, s);
                visited[level][i] = 0;
            }

        }
    }

    /**
     * 验证
     *
     * @param level
     * @param index
     * @return
     */
    public boolean invalid(int level, int index,int n,int[][] visited) {

        boolean flag = true;

        for (int i = 0; i < level; i++) {
            if (visited[i][index] == 1 || (index + level - i < n && visited[i][index + level - i] == 1) || (index - level + i >= 0 && visited[i][index - level + i] == 1)) {
                flag = false;
                break;
            }
        }

        return flag;

    }

    /**
     * 初始化棋盘
     *
     * @param n
     * @return
     */
    public List<String> init(int n) {
        ArrayList<String> res = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < n; j++) {
                sb.append(".");
            }
            res.add(sb.toString());
        }
        return res;
    }
}
