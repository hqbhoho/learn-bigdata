package com.hqbhoho.bigdata.data.structures.linklist;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/15
 */
public class LinkedListCycle {
    static class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
        }
    }

    public static void main(String[] args) {
        ListNode node1 = new ListNode(1);
        ListNode node2 = new ListNode(2);
        ListNode node3 = new ListNode(3);
        ListNode node4 = new ListNode(4);

        node1.next = node2;
        node2.next = node3;
        node3.next = node4;
        node4.next = node2;
        System.out.println(slowQuickPoint(node1));

    }

    /**
     * 快慢指针遍历   重合说明有环
     * <p>
     * +1 +2
     * 时间复杂度  O(n)
     *
     * @param node
     * @return
     */
    public static boolean slowQuickPoint(ListNode node) {

        boolean flag = false;
        if (node == null) return flag;
        ListNode slow = node;
        ListNode quick = node;
        while (quick.next != null && quick.next.next != null) {
            slow = slow.next;
            quick = quick.next.next;
            if (slow == quick) {
                flag = true;
                break;
            }
        }
        return flag;
    }
}
