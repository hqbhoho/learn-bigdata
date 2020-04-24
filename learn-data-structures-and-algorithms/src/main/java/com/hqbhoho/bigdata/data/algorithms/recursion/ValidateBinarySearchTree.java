package com.hqbhoho.bigdata.data.algorithms.recursion;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/23
 */
public class ValidateBinarySearchTree {

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

        System.out.println(inOrderValidateBinarySearchTree(node1));

    }

    /**
     * @param node
     * @param left  当前节点的左边界的值
     * @param right 当前节点的右边界的值
     * @return
     */
    public static boolean recuValidateBinarySearchTree(TreeNode node, Integer left, Integer right) {
        // 递归终止条件
        if (node == null) return true;

        // 处理当成逻辑
        if (left != null && node.val <= left) return false;
        if (right != null && node.val >= right) return false;

        // 进入下一层
        if (!recuValidateBinarySearchTree(node.left, left, node.val)) return false;
        if (!recuValidateBinarySearchTree(node.right, node.val, right)) return false;

        // 清理当前层状态
        return true;

    }


    /**
     * 中序遍历是有序的
     *
     * @param node
     * @return
     */
    public static boolean inOrderValidateBinarySearchTree(TreeNode node) {
        List<Integer> ints = new ArrayList<>();
        midOrderTraverval(node, ints);
        for (int i = 0; i < ints.size() - 1; i++) {
            if (ints.get(i) > ints.get(i + 1)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 中序遍历
     *
     * @param root
     */
    private static void midOrderTraverval(TreeNode root, List<Integer> ints) {
        if (root == null) return;
        midOrderTraverval(root.left, ints);
        ints.add(root.val);
        midOrderTraverval(root.right, ints);
    }
}
