package com.hqbhoho.bigdata.guava.collections;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;

/**
 * describe:
 *
 *
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/10/16
 */
public class BloomFilterExample {
    public static void main(String[] args) {
        BloomFilter<CharSequence> charSequenceBloomFilter =
                BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1000, 0.01);
        charSequenceBloomFilter.put("aaaa");
        charSequenceBloomFilter.put("bbbb");

        System.out.println(charSequenceBloomFilter.mightContain("aaaa"));
        System.out.println(charSequenceBloomFilter.mightContain("cc"));

    }
}
