package com.hqbhoho.bigdata.data.algorithms.recursion;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/26
 */
public class ConstructBinaryTreeFromTraversal {

    static class TreeNode {
        int val;
        TreeNode left;
        TreeNode right;

        TreeNode(int x) {
            val = x;
        }
    }

    public static void preOrder(TreeNode node) {
        if (node == null) {
            return;
        }
        System.out.println(node.val);
        preOrder(node.left);
        preOrder(node.right);
    }

    public static void main(String[] args) {
        TreeNode treeNode = recuConstructBinaryTreeFromTraversal(new int[]{3, 9, 20, 15, 7}, new int[]{9, 3, 15, 20, 7}, 0, 0, 5);
        preOrder(treeNode);
    }

    /**
     * 主要是要记录   preOrder[index]为根节点时   其在midOrder[]  中的左右子树的节点列表
     * <p>
     * eg:
     * <p>
     * preOrder : 3, 9, 20, 15, 7
     * midOrder : 9, 3, 15, 20, 7
     * <p>
     * 3 为root   其左子树在midOrder [0,0] 其根节点就是 perOrder[0+1]   左子树节点个数1（leftNum）
     * 其右子树在midOrder [2,4] 其根节点就是 perOrder[0+leftNum+1]
     *
     * @param preOrder
     * @param midOrder
     * @param startPreIndex
     * @param startMidOrderIndex
     * @param stopMidOrderIndex
     * @return
     */
    public static TreeNode recuConstructBinaryTreeFromTraversal(int[] preOrder, int[] midOrder, int startPreIndex, int startMidOrderIndex, int stopMidOrderIndex) {

        // 递归终止条件
        if (preOrder == null || midOrder == null || startPreIndex > preOrder.length - 1 || startMidOrderIndex > startMidOrderIndex) {
            return null;
        }

        int orderIndexByValue = getOrderIndexByValue(midOrder, preOrder[startPreIndex], startMidOrderIndex, stopMidOrderIndex);
        if (orderIndexByValue == -1) {
            return null;
        }

        // 处理当前层

        TreeNode curNode = new TreeNode(preOrder[startPreIndex]);
        int leftNum = orderIndexByValue - startMidOrderIndex;

        // 递归下一层
        curNode.left = recuConstructBinaryTreeFromTraversal(preOrder, midOrder, startPreIndex + 1, startMidOrderIndex, orderIndexByValue - 1);
        curNode.right = recuConstructBinaryTreeFromTraversal(preOrder, midOrder, startPreIndex + leftNum + 1, orderIndexByValue + 1, stopMidOrderIndex);
        // 清除当前层状态

        return curNode;

    }

    public static int getOrderIndexByValue(int[] midOrder, int value, int startMidOrderIndex, int stopPreOrderIndex) {
        for (int i = startMidOrderIndex; i <= stopPreOrderIndex; i++) {
            if (midOrder[i] == value) {
                return i;
            }
        }
        return -1;
    }


}
