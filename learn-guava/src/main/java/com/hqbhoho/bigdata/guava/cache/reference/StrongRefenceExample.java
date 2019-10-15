package com.hqbhoho.bigdata.guava.cache.reference;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * describe:
 * <p>
 * 强引用   不会释放内存空间  直至发生OOM
 * <p>
 * result:
 * Ref{index=96} add to list...
 * Ref{index=97} add to list...
 * Ref{index=98} add to list...
 * [Full GC (Ergonomics) [PSYoungGen: 15844K->15360K(18944K)] [ParOldGen: 86789K->86771K(87552K)] 102633K->102132K(106496K), [Metaspace: 3490K->3490K(1056768K)], 0.0251931 secs] [Times: user=0.09 sys=0.02, real=0.02 secs]
 * Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
 * [Full GC (Allocation Failure) [PSYoungGen: 15360K->15360K(18944K)] [ParOldGen: 86771K->86771K(87552K)] 102132K->102132K(106496K), [Metaspace: 3490K->3490K(1056768K)], 0.0028564 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
 * at com.hqbhoho.bigdata.guava.cache.reference.Ref.<init>(Ref.java:13)
 * Heap
 * at com.hqbhoho.bigdata.guava.cache.reference.StrongRefenceExample.main(StrongRefenceExample.java:20)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/15
 */
public class StrongRefenceExample {
    public static void main(String[] args) {

        ArrayList<Ref> list = new ArrayList<>();
        AtomicInteger count = new AtomicInteger(0);
        for (; ; ) {
            Ref ref = new Ref(count.getAndIncrement());
            list.add(ref);
            System.out.println(ref + " add to list...");
            try {
                TimeUnit.MILLISECONDS.sleep(500l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
