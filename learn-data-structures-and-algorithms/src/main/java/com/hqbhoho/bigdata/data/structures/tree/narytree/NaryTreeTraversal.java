package com.hqbhoho.bigdata.data.structures.tree.narytree;

import java.util.*;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/20
 */
public class NaryTreeTraversal {

    static class Node {
        public int val;
        public List<Node> children;

        public Node() {
        }

        public Node(int _val) {
            val = _val;
        }

        public Node(int _val, List<Node> _children) {
            val = _val;
            children = _children;
        }
    }

    public static void main(String[] args) {
        Node root = new Node(1);
        Node node1 = new Node(2);
        Node node2 = new Node(3);
        Node node3 = new Node(4);
        Node node4 = new Node(5);
        Node node5 = new Node(6);
        List<Node> children1 = Arrays.asList(node2, node1, node3);
        List<Node> children2 = Arrays.asList(node4, node5);
        root.children = children1;
        node2.children = children2;
        List<List<Integer>> res = new ArrayList<>();

//        postOrder(root, res);
        levelOrder(root, res);
        System.out.println(res);
    }

    /**
     * 前序遍历
     *
     * @param node
     * @param res
     */
    public static void preOrder(Node node, List<Integer> res) {
        if (node == null) {
            return;
        }
        res.add(node.val);
        if (node.children != null) {
            for (Node children : node.children) {
                preOrder(children, res);
            }
        }


    }

    /**
     * 后序遍历
     *
     * @param node
     * @param res
     */
    public static void postOrder(Node node, List<Integer> res) {
        if (node == null) {
            return;
        }

        if (node.children != null) {
            for (Node children : node.children) {
                postOrder(children, res);
            }
        }
        res.add(node.val);
    }


    /**
     * 层序遍历
     *
     * @param node
     * @param res
     */
    public static void levelOrder(Node node, List<List<Integer>> res) {

       Queue<Node> queue = new LinkedList<>();

        if(node != null){
            queue.add(node);
        }

        while (!queue.isEmpty()) {
            int count = queue.size();
            int depth = 0;
            ArrayList<Integer> list = new ArrayList<>();
            while (count > 0){
                Node curNode = queue.poll();
                list.add(curNode.val);
                count --;
                if (curNode.children != null) {
                    for (Node children : curNode.children) {
                        queue.add(children);
                    }
                }
            }
            res.add(list);
            depth++;
        }
    }


}
