package com.hqbhoho.bigdata.design.pattern.singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/26
 */
public class TestDemo {
    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        IntStream.rangeClosed(0, 30).forEach(i ->
                executorService.submit(() ->
                        IntStream.rangeClosed(0, 3).forEach(
                                // double check
//                                j -> System.out.println(DoubleCheckSingleTon.newInstance().hashCode())
                               // static value
//                                j -> System.out.println(StaticValueSingleton.newInstance().hashCode())
                                // inner class
//                                j -> System.out.println(StaticInnerClassSingleton.newInstance().hashCode())
                                // enum
                                j -> System.out.println(EnumSingleton.INSTANCE.hashCode())
                        )
                )
        );
        executorService.shutdown();
    }
}
