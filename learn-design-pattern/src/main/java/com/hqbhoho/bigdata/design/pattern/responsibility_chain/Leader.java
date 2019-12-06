package com.hqbhoho.bigdata.design.pattern.responsibility_chain;

public abstract class Leader {
    private Leader next;

    public void setNext(Leader next) {
        this.next = next;
    }

    public Leader getNext() {
        return next;
    }

    abstract void approveVacation(int dayNum);
}
