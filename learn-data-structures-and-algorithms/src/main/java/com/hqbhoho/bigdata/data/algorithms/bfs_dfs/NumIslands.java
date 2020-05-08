package com.hqbhoho.bigdata.data.algorithms.bfs_dfs;

import java.util.LinkedList;
import java.util.Queue;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/08
 */
public class NumIslands {
    public static void main(String[] args) {

        char[][] lands = new char[][]{
                new char[]{1, 1, 1, 1, 0},
                new char[]{1, 1, 0, 1, 0},
                new char[]{1, 1, 0, 0, 0},
                new char[]{0, 0, 0, 0, 0}
        };

        System.out.println(calcBFS(lands));

    }


    /**
     * 深度优先遍历
     *
     * @param lands
     * @return
     */
    public static int calcDFS(char[][] lands) {
        int num = 0;
        int hang = lands.length;
        int lie = 0;
        if (hang > 0) {
            lie = lands[0].length;
        }
        for (int i = 0; i < hang; i++) {
            for (int j = 0; j < lie; j++) {
                if (lands[i][j] == 1) {
                    System.out.println(num);
                    num++;
                    DFS(lands, i, j);
                }
            }
        }
        return num;
    }

    public static void DFS(char[][] lands, int i, int j) {
        int hang = lands.length;
        int lie = lands[0].length;
        if (i < 0 || j < 0 || i >= hang || j >= lie || lands[i][j] == 0) {
            return;
        }
        lands[i][j] = 0;
        DFS(lands, i + 1, j);
        DFS(lands, i - 1, j);
        DFS(lands, i, j + 1);
        DFS(lands, i, j - 1);
    }

    /**
     * 广度优先遍历
     *
     * @param lands
     * @return
     */
    public static int calcBFS(char[][] lands) {
        int num = 0;
        int hang = lands.length;
        int lie = 0;
        if (hang > 0) {
            lie = lands[0].length;
        }
        for (int i = 0; i < hang; i++) {
            for (int j = 0; j < lie; j++) {
                if (lands[i][j] == 1) {
                    num++;
                    // 广度遍历  修改有关的值为0
                    BFS(lands, i, j);
                }
            }
        }
        return num;
    }

    private static void BFS(char[][] lands, int i, int j) {
        int hang = lands.length;
        int lie = lands[0].length;
        Queue<Integer> queue = new LinkedList<>();

        queue.add(i * lie + j);

        while (!queue.isEmpty()) {
            Integer poll = queue.poll();
            int hangIndex = poll / lie;
            int lieIndex = poll % lie;

            if (hangIndex - 1 >= 0 && lands[hangIndex - 1][lieIndex] == 1) {
                lands[hangIndex - 1][lieIndex] = 0;
                queue.add((hangIndex - 1) * lie + lieIndex);
            }
            if (hangIndex + 1 < hang && lands[hangIndex + 1][lieIndex] == 1) {
                lands[hangIndex + 1][lieIndex] = 0;
                queue.add((hangIndex + 1) * lie + lieIndex);
            }
            if (lieIndex - 1 >= 0 && lands[hangIndex][lieIndex - 1] == 1) {
                lands[hangIndex][lieIndex - 1] = 0;
                queue.add(hangIndex * lie + lieIndex - 1);
            }
            if (lieIndex + 1 < lie && lands[hangIndex][lieIndex + 1] == 1) {
                lands[hangIndex][lieIndex + 1] = 0;
                queue.add(hangIndex * lie + lieIndex + 1);
            }
        }
    }


}
