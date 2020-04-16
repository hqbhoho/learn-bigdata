package com.hqbhoho.bigdata.data.structures.linklist;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/16
 */
public class MergeTwoSortedList {
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
        ListNode node4 = new ListNode(1);
        ListNode node5 = new ListNode(3);
        node1.next = node2;
        node2.next = node3;

        node4.next = node5;

        System.out.println(recuMergeTwoSortedList(node1, node4));
        //System.out.println(mergeTwoSortedList(node1, node4));

    }

    /**
     *
     * 递归求解
     *
     * @param first
     * @param second
     * @return
     */
    public static ListNode recuMergeTwoSortedList(ListNode first, ListNode second) {
        // 终止条件
        if(first == null){
           return second;
        }else if(second == null){
            return first;
        }
        if(first.val < second.val){
            first.next = recuMergeTwoSortedList(first.next,second);
            return first;
        }else{
            second.next = recuMergeTwoSortedList(first,second.next);
            return second;
        }
    }

    /**
     * 借助另外的一个链表存储结果
     * 循环遍历两个链表
     *
     * @param first
     * @param second
     * @return
     */
    public static ListNode mergeTwoSortedList(ListNode first, ListNode second) {

        ListNode res = new ListNode(0);
        ListNode resCur = res;
        ListNode cur1 = first;
        ListNode cur2 = second;
        while (cur1 != null && cur2 != null) {
            if (cur1.val > cur2.val) {
                resCur.next = cur2;
                cur2 = cur2.next;
            } else {
                resCur.next = cur1;
                cur1 = cur1.next;
            }
            resCur = resCur.next;
        }
        resCur.next = cur1 == null ? cur2 : cur1;
        return res.next;
    }
}
