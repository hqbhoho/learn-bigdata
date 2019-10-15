package com.hqbhoho.bigdata.guava.cache.LRU;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/11
 */
public class CustomerLRUCache<K,V> {

    private LinkedList<K> keys = new LinkedList();
    private Map<K,V> values = new HashMap<>();

    private static final int DEFAULT_LIMIT=10;
    private int limit;

    public CustomerLRUCache(){
        this(DEFAULT_LIMIT);
    }

    public CustomerLRUCache(int limit){
        this.limit = limit;
    }

    public void put(K key,V value){
        if(keys.contains(key)){
            keys.remove(key);
        }
        if(keys.size() >= this.limit){
            keys.removeFirst();
        }
        keys.addLast(key);
        values.put(key,value);
    }

    public V get(K key){
        if(keys.contains(key)){
            keys.remove(key);
            keys.addLast(key);
        }
        return values.get(key);
    }

    public void remove(K key){
       this.keys.remove(key);
       this.values.remove(key);
    }

    public void clear(){
        this.keys.clear();
        this.values.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for(K key : this.keys){
            sb.append(key).append(":").append(this.values.get(key)).append(",");
        }
        sb.append("]");
        return sb.toString();
    }



}
