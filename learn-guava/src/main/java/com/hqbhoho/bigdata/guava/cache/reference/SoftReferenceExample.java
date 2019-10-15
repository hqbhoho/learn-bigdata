package com.hqbhoho.bigdata.guava.cache.reference;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * describe:
 * <p>
 * 当jvm探测到即将OOM的时候, 会将SoftReference Object将会被回收，但是这个时候还是可能会发生OOM.   软引用
 * <p>
 * result:
 * Ref{index=107} add to list...
 * Ref{index=108} add to list...
 * Ref{index=109} add to list...
 * Ref{index=110} add to list...
 * Ref{index=111} add to list...
 * [GC (Allocation Failure) [PSYoungGen: 12926K->6272K(14336K)] 45842K->45364K(58368K), 0.0031770 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
 * [Full GC (Ergonomics) [PSYoungGen: 6272K->2056K(14336K)] [ParOldGen: 39092K->43128K(44032K)] 45364K->45185K(58368K), [Metaspace: 9518K->9514K(1058816K)], 0.0315432 secs] [Times: user=0.08 sys=0.01, real=0.03 secs]
 * Ref{index=112} add to list...
 * Ref{index=73} invoke finalize method and will be gc at once...
 * Ref{index=72} invoke finalize method and will be gc at once...
 * Ref{index=71} invoke finalize method and will be gc at once...
 * Ref{index=70} invoke finalize method and will be gc at once...
 * Ref{index=81} invoke finalize method and will be gc at once...
 * Ref{index=80} invoke finalize method and will be gc at once...
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/15
 */
public class SoftReferenceExample {
    public static void main(String[] args) {
        ArrayList<SoftReference<Ref>> list = new ArrayList<>();
        AtomicInteger count = new AtomicInteger(0);
        for (; ; ) {
            Ref ref = new Ref(count.getAndIncrement());
            SoftReference<Ref> refSoftReference = new SoftReference<>(ref);
            list.add(refSoftReference);
            System.out.println(refSoftReference.get() + " add to list...");
            try {
                TimeUnit.MILLISECONDS.sleep(1000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
