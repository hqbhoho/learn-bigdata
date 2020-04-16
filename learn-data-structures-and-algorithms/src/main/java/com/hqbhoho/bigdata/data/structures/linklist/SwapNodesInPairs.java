package com.hqbhoho.bigdata.data.structures.linklist;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/14
 */
public class SwapNodesInPairs {

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
        System.out.println(swapNodesInPairs(node1));

    }

    /**
     *
     * 熟练使用指针
     *
     * prev     cur    next     next_next
     * null     1  -->  2  -->      3           --> 4   --> 5
     * next.next = cur
     * cur.next = next_next
     * prev     cur    next         next_next
     * null     1  <--  2
     *          |---------------->     3           --> 4  -->  5
     * prev = cur;
     * cur = next_next;
     *         prev     next         cur(next_next)
     * null     1  <--  2
     *          |---------------->     3           --> 4  --> 5
     *  下一轮迭代
     *         prev                   cur              next   next_next
     * null     1  <--  2
     *          |---------------->     3           --> 4  --> 5
     **        prev                   cur              next   next_next
     * null     1  <--  2
     *          |---------------->     3           <-- 4
     *                                 |---------------->     5
     *
     *  prev.next = next
     **        prev                   cur              next   next_next
     * null     1  <--  2
     *          |------------------------------------> 4
                                       3   <---------- |
                                       |--------------------->     5
     *
     * @param head
     * @return
     */
    public static ListNode swapNodesInPairs(ListNode head){

        ListNode prev = null;
        ListNode cur = head;
        if(head != null && head.next != null ){
            head = head.next;
        }
        while(cur != null && cur.next != null){
            ListNode next = cur.next;
            ListNode next_next = next.next;
            next.next = cur;
            cur.next = next_next;
            if(prev != null ){
                prev.next = next;
            }
            prev = cur;
            cur = next_next;
        }
        return head;
    }
}
