package com.hqbhoho.bigdata.data.structures.linklist;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/14
 */
public class ReverseLinkedList {

    static class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
        }

        @Override
        public String toString() {
            return this.val + " --> " + (this.next == null ? "NULL" : this.next.toString());
        }
    }

    public static void main(String[] args) {
        ListNode node1 = new ListNode(1);
        ListNode node2 = new ListNode(2);
        ListNode node3 = new ListNode(3);
        ListNode node4 = new ListNode(4);
        ListNode node5 = new ListNode(5);
        node1.next = node2;
        node2.next = node3;
        node3.next = node4;
        node4.next = node5;
        System.out.println(node1);
//        System.out.println(iterReverseLinkedList(node1));
        System.out.println(recuReverseLinkedList(node1,null));


    }

    /**
     *
     * 链表的问题  一般都要借助指针求解
     * prev    cur            next
     * null     1      -->     2    -->   3
     * cur.next = prev
     * prev     cur         next
     * null <-- 1            2    -->3
     * prev = cur
     *         cur(prev)   next
     * null <-- 1            2    -->3
     * cur = next
     *        prev       next(cur)
     * null <-- 1            2    -->3
     * 下一轮迭代
     *
     * @param node
     * @return
     */
    public static ListNode iterReverseLinkedList(ListNode node) {

        if(node == null) return null;
        ListNode prev = null;
        ListNode cur = node;

        while (cur.next != null) {
            ListNode next = cur.next;
            cur.next = prev;
            prev = cur;
            cur = next;
        }
        cur.next = prev;
        return cur;
    }


    /**
     *
     * 递归实现
     * 每次传入  当前节点   及其上一个节点
     *
     * next = cur.next
     * cur.next = prev
     *
     * (next,cur)作为参数进行下一次迭代
     *
     * @param curNode
     * @param pervNode
     * @return
     */
    public static ListNode recuReverseLinkedList(ListNode curNode,ListNode pervNode) {

        if(curNode == null){
            return pervNode;
        }
        ListNode nextNode = curNode.next;
        curNode.next = pervNode;
        return recuReverseLinkedList(nextNode,curNode);

    }

}
