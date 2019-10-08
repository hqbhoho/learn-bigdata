package com.hqbhoho.bigdata.guava.collections;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * describe:
 *
 * 不可变集合
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/05
 */
public class TestImmutableCollections {

    @Test
    public void testCreate() {
        Set<Integer> ints = new HashSet<>();
        ints.addAll(Arrays.asList(1, 2, 3, 3, 4, 5, 6, 6, 6, 6));
        // builder()
        /*ImmutableSet<Integer> immutableSet = ImmutableSet.<Integer>builder()
                .add(3)
                .addAll(ints)
                .build();*/
        // copyof
        /*ImmutableSet<Integer> immutableSet = ImmutableSet.copyOf(ints);*/
        // of
        ImmutableSet<Integer> immutableSet = ImmutableSet.of(1, 2, 3, 3, 4, 5, 6, 6, 6, 6);
        assertThat(immutableSet.size(), equalTo(6));
        ints.add(7);
        assertThat(immutableSet.size(), equalTo(6));
    }
}
