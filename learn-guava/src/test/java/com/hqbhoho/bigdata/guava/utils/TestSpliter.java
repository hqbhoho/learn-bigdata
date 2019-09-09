package com.hqbhoho.bigdata.guava.utils;

import com.google.common.base.Splitter;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * describe:
 * <p>
 * String  ===> Collection   Spilt Operator
 * 字符串切割
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/04
 */
public class TestSpliter {

    @Test
    public void test_split_on_split(){
        List<String> result = Splitter.on("#").splitToList("A#B##C");
        assertThat(result.size(), equalTo(4));
    }

    @Test
    public void test_split_on_split_skipEmptyValue(){
        List<String> result = Splitter.on("#").omitEmptyStrings().splitToList("A#B##C");
        assertThat(result.size(), equalTo(3));
    }

    @Test
    public void test_split_on_split_skip_and_trim_EmptyValue(){
        List<String> result = Splitter.on("#").omitEmptyStrings().trimResults().splitToList("A #B ## C");
        assertThat(result.get(0), equalTo("A"));
        assertThat(result.get(1), equalTo("B"));
        assertThat(result.get(2), equalTo("C"));
    }

    @Test
    public void test_split_on_split_skip_and_trim_EmptyValue_to_map(){
        Map<String,String> result = Splitter.on("#").omitEmptyStrings().withKeyValueSeparator("=")
                .split("A=A#B=B##C=C");
        assertThat(result.get("A"), equalTo("A"));
        assertThat(result.get("B"), equalTo("B"));
        assertThat(result.get("C"), equalTo("C"));
    }
}
