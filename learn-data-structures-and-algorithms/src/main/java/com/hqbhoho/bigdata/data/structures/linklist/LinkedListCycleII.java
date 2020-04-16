package com.hqbhoho.bigdata.data.structures.linklist;

import java.util.HashSet;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/15
 */
public class LinkedListCycleII {
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
        ListNode node5 = new ListNode(5);

        node1.next = node2;
        node2.next = node3;
        node3.next = node4;
        node4.next = node5;
        node5.next = node2;
//        System.out.println(visitedNode(node1));
        System.out.println(slowQuickPoint(node1));

    }

    /**
     * 2(a+b) = a+b +c +b  -> a=c
     *
     * @param node
     * @return
     */
    public static ListNode slowQuickPoint(ListNode node) {

        if(node == null) return null;
        ListNode slow = node;
        ListNode quick = node;
        while (quick.next != null && quick.next.next != null) {
            slow = slow.next;
            quick = quick.next.next;
            if (slow == quick) {
                ListNode slow2 = node;
                while (slow != slow2) {
                    slow = slow.next;
                    slow2 = slow2.next;
                }
                return slow2;
            }
        }
        return null;

    }

    /**
     * HashSet存放 visited node
     * 时间复杂度  O(n)
     * 空间复杂度  O(1)
     *
     * @param node
     * @return
     */
    public static ListNode visitedNode(ListNode node) {
        HashSet<ListNode> set = new HashSet<>();
        ListNode cur = node;
        while (cur != null) {
            if (set.contains(cur)) {
                return cur;
            }
            set.add(cur);
            cur = cur.next;
        }
        return null;
    }
}
