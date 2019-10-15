package com.hqbhoho.bigdata.guava.cache.reference;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/15
 */
public class Ref {

    private int index;
    private byte[] data = new byte[1024*1024];

    public Ref(int index) {
        this.index = index;
    }

    @Override
    protected void finalize() throws Throwable {
        System.out.println(this+" invoke finalize method and will be gc at once...");
    }

    @Override
    public String toString() {
        return "Ref{" +
                "index=" + index +
                '}';
    }
}
