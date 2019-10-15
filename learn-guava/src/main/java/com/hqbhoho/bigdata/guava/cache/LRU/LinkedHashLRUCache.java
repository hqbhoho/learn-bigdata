package com.hqbhoho.bigdata.guava.cache.LRU;

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * describe:
 * <p>
 * L
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/11
 */
public class LinkedHashLRUCache<K, V> {

    private LinkedHashMap<K, V> cache;
    private static final int DEFAULT_LIMIT = 10;

    public LinkedHashLRUCache() {
        this(DEFAULT_LIMIT);
    }

    public LinkedHashLRUCache(int limit) {
        cache = new InternalLinkedHashMap<>(limit);
    }

    public void put(K key,V value){
        this.cache.put(key,value);
    }

    public V get(K key){
        return this.cache.get(key);
    }

    public void remove(K key){
        this.cache.remove(key);
    }

    public void clear(){
        this.cache.clear();
    }

    @Override
    public String toString() {
        return cache.toString();
    }

    static class InternalLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
        private int limit;

        private InternalLinkedHashMap(int limit) {
            super(16, 0.75f, true);
            Preconditions.checkState(limit > 0, "limit must gt 0...");
            this.limit = limit;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
            return this.size() > this.limit;
        }
    }
}
