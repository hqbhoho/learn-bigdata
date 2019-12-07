package com.hqbhoho.bigdata.design.pattern.iterator;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class TestDemo {
    public static void main(String[] args) {
        BookShelf computorBookShelf = new ComputorBookShelf();
        computorBookShelf.addBook("Java In Action");
        computorBookShelf.addBook("Scala In Action");
        computorBookShelf.addBook("Python In Action");
        computorBookShelf.addBook("C In Action ");
        Iterator bookShelfIterator = computorBookShelf.getBookShelfIterator();
        while(bookShelfIterator.hasNext()){
            System.out.println(bookShelfIterator.next());
        }
    }
}
