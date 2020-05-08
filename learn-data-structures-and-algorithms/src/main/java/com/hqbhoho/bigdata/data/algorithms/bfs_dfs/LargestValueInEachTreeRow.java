package com.hqbhoho.bigdata.data.algorithms.bfs_dfs;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/30
 */
public class LargestValueInEachTreeRow {

    static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;

        TreeNode(int x) {
            val = x;
        }
    }

    public static void main(String[] args) {
        TreeNode root = new TreeNode(3);
        TreeNode node1 = new TreeNode(9);
        TreeNode node2 = new TreeNode(20);
        TreeNode node3 = new TreeNode(15);
        TreeNode node4 = new TreeNode(7);
        root.left = node1;

        root.right = node2;
        node2.left = node3;
        node2.right = node4;

        System.out.println(BFS(root));

    }

    /**
     * 广度优先遍历
     *
     * @param root
     * @return
     */
    public static List<Integer> BFS(TreeNode root) {
        List<Integer> res = new ArrayList<>();
        LinkedList<TreeNode> queue = new LinkedList<>();
        if (root == null) {
            return res;
        }

        queue.addFirst(root);
        while (queue.size() != 0) {
            int max = Integer.MIN_VALUE;
            int count = queue.size();
            while (count > 0) {
                TreeNode treeNode = queue.removeLast();
                max = Math.max(max, treeNode.val);
                if (treeNode.left != null) queue.addFirst(treeNode.left);
                if (treeNode.right != null) queue.addFirst(treeNode.right);
                count--;
            }
            res.add(max);
        }
        return res;
    }
}
