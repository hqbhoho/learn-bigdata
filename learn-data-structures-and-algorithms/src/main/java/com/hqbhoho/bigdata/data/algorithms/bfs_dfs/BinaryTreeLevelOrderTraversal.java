package com.hqbhoho.bigdata.data.algorithms.bfs_dfs;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/30
 */
public class BinaryTreeLevelOrderTraversal {


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

//        System.out.println(BFS(root));
        DFS(root);
        stackDFS(root);


    }

    /**
     * 广度优先遍历
     *
     * @param node
     */
    public static List<List<Integer>> BFS(TreeNode node) {
        List<List<Integer>> res = new ArrayList<>();
        LinkedList<TreeNode> queue = new LinkedList<>();
        if (node == null) {
            return res;
        }

        queue.addFirst(node);
        while (queue.size() != 0) {
            ArrayList<Integer> temp = new ArrayList<>();
            int count = queue.size();
            while (count > 0) {
                TreeNode treeNode = queue.removeLast();
                temp.add(treeNode.val);
                if (treeNode.left != null) queue.addFirst(treeNode.left);
                if (treeNode.right != null) queue.addFirst(treeNode.right);
                count--;
            }
            res.add(temp);
        }
        return res;
    }

    /**
     * 深度优先遍历
     * <p>
     * 基于递归
     *
     * @param node
     */
    public static void DFS(TreeNode node) {
        if (node == null) {
            return;
        }
        System.out.println(node.val);
        DFS(node.left);
        DFS(node.right);
    }

    /**
     * 深度优先遍历
     * <p>
     * 基于栈的深度优先遍历
     *
     * @param node
     */
    public static void stackDFS(TreeNode node) {
        if (node == null) {
            return;
        }
        Stack<TreeNode> stack = new Stack<>();

        stack.push(node);

        while (!stack.empty()) {
            TreeNode cur = stack.pop();
            System.out.println(cur.val);
            if (cur.right != null) {
                stack.push(cur.right);
            }

            if (cur.left != null) {
                stack.push(cur.left);
            }

        }


    }


}
