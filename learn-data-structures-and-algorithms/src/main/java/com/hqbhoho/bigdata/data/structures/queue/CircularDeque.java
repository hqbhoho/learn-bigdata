package com.hqbhoho.bigdata.data.structures.queue;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/18
 */
public class CircularDeque {

    public static void main(String[] args) {
        /**
         *
         * ["MyCircularDeque","insertFront","deleteLast","getRear","getFront","getFront","deleteFront","insertFront","insertLast","insertFront","getFront","insertFront"]
         [[4],[9],[],[],[],[],[],[6],[5],[9],[],[6]]
         *
         */
        CircularDeque circularDeque = new CircularDeque(4); // set the size to be 3
        System.out.println(circularDeque.insertLast(9));
        System.out.println(circularDeque.deleteLast());
        System.out.println(circularDeque.getRear());
        System.out.println(circularDeque.getFront());
        System.out.println(circularDeque.getFront());
        System.out.println(circularDeque.deleteFront());

        System.out.println(circularDeque.insertFront(6));
        System.out.println(circularDeque.insertLast(5));
        System.out.println(circularDeque.insertFront(9));

        System.out.println(circularDeque.getFront());
        System.out.println(circularDeque.insertFront(6));

    }

    private Node first;
    private Node last;
    private int maxSize;
    private int curSize;

    static class Node {
        int val;
        Node next;
        Node prev;

        public Node(int val) {
            this.val = val;
        }
    }

    /**
     * Initialize your data structure here. Set the size of the deque to be k.
     */
    public CircularDeque(int k) {
        this.maxSize = k;
    }

    /**
     * Adds an item at the front of Deque. Return true if the operation is successful.
     */
    public boolean insertFront(int value) {
        if (isFull()) {
            return false;
        }
        Node node = new Node(value);
        if (first == null) {
            first = node;
            last = node;
        } else {
            first.prev = node;
            node.next = first;
            node.prev = last;
            last.next = node;
            first = node;
        }
        this.curSize++;
        return true;
    }

    /**
     * Adds an item at the rear of Deque. Return true if the operation is successful.
     */
    public boolean insertLast(int value) {
        if (isFull()) {
            return false;
        }
        Node node = new Node(value);
        if (first == null) {
            first = node;
            last = node;
        } else {
            last.next = node;
            node.prev = last;
            node.next = first;
            first.prev = node;
            last = node;
        }
        this.curSize++;
        return true;
    }

    /**
     * Deletes an item from the front of Deque. Return true if the operation is successful.
     */
    public boolean deleteFront() {
        if (isEmpty()) {
            return false;
        }
        if (this.curSize == 1) {
            first = null;
            last = null;
            this.curSize --;
            return true;
        }
        if (last != null && first.next != null) {
            Node next = first.next;
            next.prev = last;
            last.next = next;
            first = next;
            this.curSize --;
        }
        return true;
    }

    /**
     * Deletes an item from the rear of Deque. Return true if the operation is successful.
     */
    public boolean deleteLast() {
        if (isEmpty()) {
            return false;
        }
        if (this.curSize == 1) {
            first = null;
            last = null;
            this.curSize --;
            return true;
        }
        if (first != null && last.prev != null) {
            Node prev = last.prev;
            prev.next = first;
            first.prev = prev;
            this.curSize --;
            last = prev;
        }
        return true;

    }

    /**
     * Get the front item from the deque.
     */
    public Integer getFront() {
        if(first == null) return null;
        return first.val;

    }

    /**
     * Get the last item from the deque.
     */
    public Integer getRear() {
        if(first == null) return null;
        return last.val;
    }

    /**
     * Checks whether the circular deque is empty or not.
     */
    public boolean isEmpty() {
        return this.curSize == 0;
    }

    /**
     * Checks whether the circular deque is full or not.
     */
    public boolean isFull() {
        return this.curSize == maxSize;
    }
}
