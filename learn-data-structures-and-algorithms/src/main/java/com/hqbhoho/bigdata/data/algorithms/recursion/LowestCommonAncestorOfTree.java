package com.hqbhoho.bigdata.data.algorithms.recursion;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/25
 */
public class LowestCommonAncestorOfTree {
    private TreeNode res;

    static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;

        TreeNode(int x) {
            val = x;
        }

        @Override
        public String toString() {
            return String.valueOf(this.val);
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

        LowestCommonAncestorOfTree lowestCommonAncestorOfTree = new LowestCommonAncestorOfTree();
        lowestCommonAncestorOfTree.recuCalcLowestCommonAncestorOfTree(node1, node6, node7);
        System.out.println(lowestCommonAncestorOfTree.res);


    }

    /**
     * 当前层  或者其左子树   或者有子树   三者两个为true  则当前节点是最小公共祖先
     *
     * @param root
     * @param left
     * @param right
     * @return
     */
    public boolean recuCalcLowestCommonAncestorOfTree(TreeNode root, TreeNode left, TreeNode right) {

        // 递归终止条件
        if (root == null) return false;
        // 处理当前层
        int curContains = 0;
        if (root == left || root == right) {
            curContains = 1;
        }

        // 递归下一层

        int leftContains = recuCalcLowestCommonAncestorOfTree(root.left, left, right) ? 1 : 0;
        int rightContains = recuCalcLowestCommonAncestorOfTree(root.right, left, right) ? 1 : 0;

        // 处理当前状态

        if (curContains + leftContains + rightContains == 2) {
            this.res = root;
        }

        //  当前节点及其子树是否发现
        return curContains + leftContains + rightContains > 0;
    }

}
