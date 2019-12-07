package com.hqbhoho.bigdata.design.pattern.iterator;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class ComputorBookShelf implements BookShelf {

    private List<String> books = new ArrayList<>();
    @Override
    public String getBookAtIndex(int index) {
        return this.books.get(index);
    }

    @Override
    public void addBook(String name) {
        this.books.add(name);
    }

    @Override
    public int getBookCount() {
        return this.books.size();
    }

    @Override
    public Iterator getBookShelfIterator() {
        return new BookShelfIterator(this);
    }
}
