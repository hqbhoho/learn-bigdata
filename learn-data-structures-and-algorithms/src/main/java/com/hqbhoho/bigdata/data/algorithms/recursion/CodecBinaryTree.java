package com.hqbhoho.bigdata.data.algorithms.recursion;

import java.util.*;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/25
 */
public class CodecBinaryTree {
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
        TreeNode node = new Codec().deserialize("5,2,3,null,null,2,4,3,1");
        List<Integer> res = new ArrayList<>();
        preOrder(node, res);
        System.out.println(res);
    }

    /**
     * 前序遍历
     *
     * @param node
     * @param res
     */
    public static void preOrder(TreeNode node, List<Integer> res) {
        if (node == null) {
            return;
        }
        res.add(node.val);
        preOrder(node.left, res);
        preOrder(node.right, res);
    }

    static class Codec {

        // Encodes a tree to a single string.
        public String serialize(TreeNode root) {
            StringBuilder sb = new StringBuilder();
            levelOrder(root, sb);
            return sb.toString();

        }

        // Decodes your encoded data to tree.
        public TreeNode deserialize(String data) {
            String[] split = data.split(",");
            if(split.length == 0 || split[0].equals("")) return null;
            List<TreeNode> nodes = new ArrayList<>();
            nodes.add(new TreeNode(Integer.valueOf(split[0])));


            int nodeIndex = 0;
            int valueIndex = 1;


            while(nodeIndex < nodes.size()){
                TreeNode temp = nodes.get(nodeIndex);
                if(valueIndex >= split.length){
                    break;
                }
                String left = split[valueIndex++];
                String right = split[valueIndex++];
                if(!left.equalsIgnoreCase("null")){
                    temp.left = new TreeNode(Integer.valueOf(left));
                    nodes.add(temp.left);
                }
                if(!right.equalsIgnoreCase("null")){
                    temp.right = new TreeNode(Integer.valueOf(right));
                    nodes.add(temp.right);
                }

                nodeIndex++;
            }



            return nodes.get(0);
        }



        /**
         * 层序遍历
         *
         * @param node
         * @param sb
         */
        public void levelOrder(TreeNode node, StringBuilder sb) {

            Queue<TreeNode> queue = new LinkedList<>();

            if (node != null) {
                queue.add(node);
            }else{
                return ;
            }

            while (!queue.isEmpty()) {

                TreeNode curNode = queue.poll();

                if (curNode != null) {
                    sb.append(curNode.val + ",");
                    queue.add(curNode.left);
                    queue.add(curNode.right);
                    continue;

                }
                sb.append("null,");
            }
            sb.deleteCharAt(sb.length() - 1);
        }
    }
}
