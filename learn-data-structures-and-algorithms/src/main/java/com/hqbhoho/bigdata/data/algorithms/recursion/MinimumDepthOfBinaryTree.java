package com.hqbhoho.bigdata.data.algorithms.recursion;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/25
 */
public class MinimumDepthOfBinaryTree {
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
//        node1.right = node3;
//        node2.left = node4;
//        node2.right = node5;
//        node3.left = node6;
//        node3.right = node7;


        System.out.println(recuCalc(node1));


    }

    public static int recuCalc(TreeNode node) {
        if (node == null) return 0;
        return recuCalcMinimumDepthOfBinaryTree(node, 1);
    }

    /**
     * @param node
     * @param height
     * @return
     */
    public static int recuCalcMinimumDepthOfBinaryTree(TreeNode node, int height) {

        // 递归终止条件  叶子节点
        if (node.left == null && node.right == null) {
            return height;
        }

        // 处理当前节点
        // 递归下一层
        if (node.left != null && node.right != null) {
            int left = recuCalcMinimumDepthOfBinaryTree(node.left, height + 1);
            int right = recuCalcMinimumDepthOfBinaryTree(node.right, height + 1);
            return left < right ? left : right;
        }

        if (node.left != null) {
            return recuCalcMinimumDepthOfBinaryTree(node.left, height + 1);
        }
        return recuCalcMinimumDepthOfBinaryTree(node.right, height + 1);
        // 清除状态

    }
}
