package com.hqbhoho.bigdata.data.algorithms.recursion;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/23
 */
public class InvertBinaryTree {

    static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;

        TreeNode(int x) {
            val = x;
        }
    }

    public static void main(String[] args) {
        TreeNode node1 = new TreeNode(4);
        TreeNode node2 = new TreeNode(2);
        TreeNode node3 = new TreeNode(7);
        TreeNode node4 = new TreeNode(1);
        TreeNode node5 = new TreeNode(3);
        TreeNode node6 = new TreeNode(6);
        TreeNode node7 = new TreeNode(9);
        node1.left = node2;
        node1.right = node3;
        node2.left = node4;
        node2.right = node5;
        node3.left = node6;
        node3.right = node7;
        recuInvertBinaryTree(node1);
        midOrderTraverval(node1);


    }

    /**
     * @param node
     */
    private static void recuInvertBinaryTree(TreeNode node) {
        if (node == null) return;

        // 递归终止条件
        if (node.left == null && node.right == null) {
            return;
        }
        // 处理当前层

        TreeNode temp = node.left;
        node.left = node.right;
        node.right = temp;

        // 进入下一层

        recuInvertBinaryTree(node.left);
        recuInvertBinaryTree(node.right);

        // 清除当前状态
    }


    private static void midOrderTraverval(TreeNode root) {
        if (root == null) return;
        midOrderTraverval(root.left);
        System.err.println(root.val);
        midOrderTraverval(root.right);
    }


}
