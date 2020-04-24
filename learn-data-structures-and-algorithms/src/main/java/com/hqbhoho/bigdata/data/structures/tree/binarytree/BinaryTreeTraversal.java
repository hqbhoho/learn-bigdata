package com.hqbhoho.bigdata.data.structures.tree.binarytree;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/20
 */
public class BinaryTreeTraversal {
    static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;

        TreeNode(int x) {
            val = x;
        }

    }

    public static void main(String[] args) {
        TreeNode root = new TreeNode(1);
        TreeNode node1 = new TreeNode(2);
        TreeNode node2 = new TreeNode(3);
        root.right = node1;
        node1.left = node2;
        List<Integer> res = new ArrayList<>();
        midOrder(root,res);
        System.out.println(res);

    }

    /**
     * 前序遍历
     *
     * @param node
     * @param res
     *
     */
    public static void preOrder(TreeNode node,List<Integer> res){
        if(node ==null){
            return;
        }
        res.add(node.val);
        preOrder(node.left,res);
        preOrder(node.right,res);
    }

    /**
     * 中序遍历
     *
     * @param node
     * @param res
     *
     */
    public static void midOrder(TreeNode node,List<Integer> res){
        if(node ==null){
            return;
        }

        midOrder(node.left,res);
        res.add(node.val);
        midOrder(node.right,res);
    }

    /**
     * 后序遍历
     *
     * @param node
     * @param res
     *
     */
    public static void postOrder(TreeNode node,List<Integer> res){
        if(node ==null){
            return;
        }

        postOrder(node.left,res);
        postOrder(node.right,res);
        res.add(node.val);
    }


}
