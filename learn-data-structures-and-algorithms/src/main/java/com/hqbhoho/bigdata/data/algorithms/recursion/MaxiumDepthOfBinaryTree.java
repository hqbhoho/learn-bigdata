package com.hqbhoho.bigdata.data.algorithms.recursion;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/25
 */
public class MaxiumDepthOfBinaryTree {

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


        System.out.println(recuCalcMaxiumDepthOfBinaryTree(node1,0));


    }

    public static int recuCalcMaxiumDepthOfBinaryTree(TreeNode node,int height) {
        //递归终止条件
        if(node == null){
            return height;
        }
        // 处理当前层
        height++;
        // 进入下一层
        int left = recuCalcMaxiumDepthOfBinaryTree(node.left, height);
        int right = recuCalcMaxiumDepthOfBinaryTree(node.right,height);

        // 清理当前层状态

        return left>right?left:right;

    }
}
