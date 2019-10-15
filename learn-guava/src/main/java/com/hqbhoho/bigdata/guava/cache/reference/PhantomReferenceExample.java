package com.hqbhoho.bigdata.guava.cache.reference;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 * <p>
 * 幻影引用
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/15
 */
public class PhantomReferenceExample {
    public static void main(String[] args) throws InterruptedException {
        Ref ref = new Ref(10);
        ReferenceQueue<Ref> referenceQueue = new ReferenceQueue<>();
        PhantomReference<Ref> phantomReference = new ObjectTracker<Ref>(ref, referenceQueue,10+"");
        ref = null;
        System.gc();
        Thread t1 = new Thread(() -> {
            Reference<? extends Ref> poll = null;
            while(null == poll){
                poll = referenceQueue.poll();
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Ref index: "+ ((ObjectTracker)poll).getIndex() + " is collected...");
        });

        t1.start();
        t1.join();
    }

    static class ObjectTracker<T> extends PhantomReference<T>{

        private String index ;

        public ObjectTracker(T t, ReferenceQueue<? super T> referenceQueue,String index) {
            super(t, referenceQueue);
            this.index = index;
        }

        public String getIndex() {
            return this.index;
        }
    }


}
