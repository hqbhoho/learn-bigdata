package com.hqbhoho.bigdata.data.structures.stack;

import java.util.EmptyStackException;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/17
 */
public class MinStack {
    public static void main(String[] args) {
        MinStack minStack = new MinStack();
        minStack.push(-2);
        minStack.push(0);
        minStack.push(-1);
        System.out.println(minStack.getMin());

        System.out.println(minStack.top());
        minStack.pop();
        System.out.println(minStack.getMin());
    }

    static class Node {
        int val;
        Node next;
        int curMin;

        public Node(int val) {
            this.val = val;
        }
    }

    private Node first;

    /**
     * initialize your data structure here.
     */
    public MinStack() {
    }

    public void push(int x) {
        Node cur = new Node(x);
        cur.next = first;
        if(first != null && cur.val >= first.curMin){
            cur.curMin = first.curMin;
        }else{
            cur.curMin = cur.val;
        }
        first = cur;
    }

    public void pop() {
        if (first == null) throw new EmptyStackException();
        Node node = first;
        first = first.next;
        node.next = null;
    }

    public int top() {
        if (first == null) throw new EmptyStackException();
        return first.val;
    }

    public int getMin() {
        return first.curMin;
    }
}
