package com.hqbhoho.bigdata.design.pattern.iterator;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class BookShelfIterator implements Iterator {
    private BookShelf bookShelf;
    private int index;

    public BookShelfIterator(BookShelf bookShelf) {
        this.bookShelf = bookShelf;
        this.index = 0;
    }

    @Override
    public boolean hasNext() {
        return index < bookShelf.getBookCount();
    }

    @Override
    public Object next() {
        Object res = bookShelf.getBookAtIndex(index);
        index++;
        return res;
    }
}
