package com.hqbhoho.bigdata.data.structures.linklist;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/15
 */
public class ReverseNodesInKGroup {
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
        System.out.println(reverseNodesInKGroup(node1, 10));
    }

    /**
     *
     * 每num个元素的链表反转  之后  再将各个反转之后的链表进行链接
     *
     *
     * @param head
     * @param num
     * @return
     */
    public static ListNode reverseNodesInKGroup(ListNode head, int num) {
        if (head == null) return null;
        ListNode res = head;
        ListNode cur = head;
        ListNode first = null;
        ListNode prev = null;
        int k = 1;
        while (cur != null) {
            ListNode next = cur.next;
            if (k == num) {
                res = cur;
            }
            if (k % num == 1) {
                if (prev != null)
                    prev.next = cur;
                first = cur;
            }
            if (k % num == 0) {
                iterReverseLinkedList(first, cur);
                if (prev != null) prev.next = cur;
                prev = first;
            }
            cur = next;
            k++;
        }
        return res;
    }

    public static ListNode iterReverseLinkedList(ListNode start, ListNode end) {

        if (start == null) return null;
        ListNode prev = null;
        ListNode cur = start;

        while (cur != end) {
            ListNode next = cur.next;
            cur.next = prev;
            prev = cur;
            cur = next;
        }
        cur.next = prev;
        return cur;
    }
}
