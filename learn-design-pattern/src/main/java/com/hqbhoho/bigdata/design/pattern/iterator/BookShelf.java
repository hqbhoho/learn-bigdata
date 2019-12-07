package com.hqbhoho.bigdata.design.pattern.iterator;

public interface BookShelf {
    String getBookAtIndex(int index);
    void addBook(String name);
    int getBookCount();
    Iterator getBookShelfIterator();
}
