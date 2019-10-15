package com.hqbhoho.bigdata.guava.cache;

import com.google.common.cache.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 * <p>
 * Guava Cache
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/15
 */
public class CacheExample {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

//        test1();
        test2();
    }

    private static void test2() throws ExecutionException, InterruptedException {
        LoadingCache<String, Student> cache = CacheBuilder.newBuilder()
                .maximumSize(3)
                .weakValues()
                .weakKeys()
//                .refreshAfterWrite(1,TimeUnit.SECONDS)
                .expireAfterWrite(1,TimeUnit.SECONDS)
                .removalListener(new RemovalListener<String, Student>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, Student> notification) {
                        System.out.println("Removal reason: " + notification.getCause().name());
                        System.out.println("Collect object is: " + notification.getValue());
                    }
                })
                .build(new CacheLoader<String, Student>() {
                    @Override
                    public Student load(String key) throws Exception {
                        System.out.println("======================");
                        return new Student(key, key);
                    }
                });

        cache.get("haqhoho002");
        cache.get("haqhoho003");
        cache.get("haqhoho004");
        TimeUnit.SECONDS.sleep(2);

        System.out.println(cache.getIfPresent("hqbhoho01"));
        System.out.println(cache.getIfPresent("hqbhoho02"));
        cache.get("haqhoho001");
        cache.invalidate("haqhoho001");
    }

    private static void test1() throws InterruptedException {
        LoadingCache<String, Student> cache = CacheBuilder.newBuilder()
                .maximumWeight(40)
                .weigher(new Weigher<String, Student>() {
                    @Override
                    public int weigh(String key, Student value) {
                        return value.getId().length() + value.getName().length();
                    }
                })
                .expireAfterWrite(3, TimeUnit.SECONDS)
                .concurrencyLevel(1)
                .recordStats()
                .removalListener(new RemovalListener<String, Student>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, Student> notification) {
                        System.out.println("Removal reason: " + notification.getCause().name());
                        System.out.println("Collect object is: " + notification.getValue());
                    }
                })
                .build(new CacheLoader<String, Student>() {
                    @Override
                    public Student load(String key) throws Exception {
                        return new Student(key, key);
                    }
                });

        cache.getUnchecked("hqbhoho01");
        cache.getUnchecked("hqbhoho02");
        TimeUnit.SECONDS.sleep(2);
        cache.getUnchecked("hqbhoho02");
        TimeUnit.SECONDS.sleep(4);
        System.out.println(cache.getIfPresent("hqbhoho01"));
        System.out.println(cache.getIfPresent("hqbhoho02"));
        CacheStats stats = cache.stats();
        System.out.println(stats.hitCount()+":"+stats.hitRate());
        System.out.println(stats.evictionCount());
    }
}
