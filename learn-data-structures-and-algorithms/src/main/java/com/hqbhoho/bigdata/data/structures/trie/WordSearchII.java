package com.hqbhoho.bigdata.data.structures.trie;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/08
 */
public class WordSearchII {
    public static void main(String[] args) {
        char[][] board = new char[][]{
                new char[]{'o', 'a', 'a', 'n'},
                new char[]{'e', 't', 'a', 'e'},
                new char[]{'i', 'h', 'k', 'r'},
                new char[]{'i', 'f', 'l', 'v'}
        };
        String[] words = new String[]{"oath", "pea", "eat", "rain"};
        System.out.println(calc(board, words));

    }

    public static List<String> calc(char[][] board, String[] words) {
        Trie tire = new Trie();
        List<String> res = new ArrayList<>();

        if (board == null || board.length == 0 || board[0] == null || board[0].length == 0
                || words == null || words.length == 0) {
            return res;
        }

        // 将字符创插入到Trie树中
        for (String word : words) {
            tire.insert(word);
        }


        for (int i = 0; i < board.length; i++) {
            for (int j = 0; j < board[i].length; j++) {
                existRecursive(board, i, j, tire.root, res);
            }
        }


        return res;

    }

    // 遍历
    private static void existRecursive(char[][] board, int row, int col, Trie.TrieNode node, List<String> res) {

        // 递归终止条件
        if (row < 0 || row >= board.length || col < 0 || col >= board[0].length)
            return;

        if (node.content.containsKey(board[row][col])) {
            Trie.TrieNode cur = node.content.get(board[row][col]);
            // 避免重复
            if (cur.isLeaf && cur.word != null) {
                res.add(cur.word);
                cur.word = null;
            }

            char temp = board[row][col];
            //上下左右去遍历
            board[row][col] = '$';
            existRecursive(board, row - 1, col, cur, res);
            existRecursive(board, row + 1, col, cur, res);
            existRecursive(board, row, col - 1, cur, res);
            existRecursive(board, row, col + 1, cur, res);
            board[row][col] = temp;
        }

    }
}
