package com.hqbhoho.bigdata.data.structures.trie;

import java.util.HashMap;
import java.util.Map;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/08
 */
public class Trie {

    public  TrieNode root;

    static class TrieNode {
        // 是否是叶子节点
        public boolean isLeaf;
        public Map<Character, TrieNode> content;
        public String word;

        public TrieNode(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.content = new HashMap<>();
        }

        public TrieNode() {
            this(false);
        }

        public TrieNode(String word) {
            this(true);
            this.word = word;
        }
    }


    /**
     * Initialize your data structure here.
     */
    public Trie() {
        this.root = new TrieNode();
    }

    /**
     * Inserts a word into the trie.
     */
    public void insert(String word) {
        char[] chars = word.toCharArray();
        TrieNode cur = this.root;
        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];
            if (!cur.content.containsKey(ch)) {
                if (i == chars.length - 1) {
                    cur.content.put(ch,new TrieNode(word));
                } else {
                    cur.content.put(ch,new TrieNode());
                }
            }
            cur = cur.content.get(ch);
            if(i == chars.length - 1 && !cur.isLeaf){
                cur.isLeaf = true;
                cur.word = word;
            }
        }

    }

    /**
     * Returns if the word is in the trie.
     */
    public boolean search(String word) {

        if(word == null || word.isEmpty()) return false;
        TrieNode cur = this.root;
        char[] chars = word.toCharArray();
        for(int i = 0; i < chars.length; i++){
            char ch = chars[i];
            if(!cur.content.containsKey(ch)) return false;
            cur =  cur.content.get(ch);
            if(i == chars.length - 1 && !cur.isLeaf) return false;
        }
        return true;

    }

    /**
     * Returns if there is any word in the trie that starts with the given prefix.
     */
    public boolean startsWith(String prefix) {
        if(prefix == null || prefix.isEmpty()) return false;
        TrieNode cur = this.root;
        char[] chars = prefix.toCharArray();
        for(int i = 0; i < chars.length; i++){
            char ch = chars[i];
            if(!cur.content.containsKey(ch)) return false;
            cur =  cur.content.get(ch);
        }
        return true;
    }

    public static void main(String[] args) {
        Trie trie = new Trie();

        trie.insert("apple");
        System.out.println(trie.search("apple"));   // 返回 true
        System.out.println(trie.search("app"));     // 返回 false
        System.out.println(trie.startsWith("app")); // 返回 true
        trie.insert("app");
        System.out.println(trie.search("app"));     // 返回 true


    }
}
