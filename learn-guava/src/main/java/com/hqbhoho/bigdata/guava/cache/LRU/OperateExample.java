package com.hqbhoho.bigdata.guava.cache.LRU;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/14
 */
public class OperateExample {
    public static void main(String[] args) {
//        LinkedHashLRUCache<String, String> cache = new LinkedHashLRUCache<>(3);
        CustomerLRUCache<String,String> cache = new CustomerLRUCache<>(3);
        cache.put("1", "1");
        cache.put("2", "2");
        cache.put("3", "3");
        cache.put("1", "11");
        System.out.println(cache);
        cache.get("1");
        System.out.println(cache);
        cache.put("4","4");
        System.out.println(cache);


    }
}
