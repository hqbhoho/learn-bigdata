package com.hqbhoho.bigdata.guava.utils;

import com.google.common.base.Joiner;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * describe:
 * <p>
 * Collection ==> String     Join Operator
 * 字符串拼接
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/04
 */
public class TestJoiner {

    @Test
    public void test_Join_on_Join() {
        String result = Joiner.on("#").join(Arrays.asList("A", "B", "C", "D"));
        assertThat(result, equalTo("A#B#C#D"));
    }

    @Test(expected = NullPointerException.class)
    public void test_Join_on_Join_WithNullValue() {
        String result = Joiner.on("#").join(Arrays.asList("A", "B", "C", "D", null));
        assertThat(result, equalTo("A#B#C#D"));
    }

    @Test
    public void test_Join_on_Join_SkipNullValue() {
        String result = Joiner.on("#").skipNulls().join(Arrays.asList("A", "B", "C", "D", null));
        assertThat(result, equalTo("A#B#C#D"));
    }

    @Test
    public void test_Join_on_Join_MapValue() {
        Map map = new HashMap();
        map.put("A", "A");
        map.put("B", "B");
        map.put("C", "C");
        String result = Joiner.on("#").withKeyValueSeparator("=").join(map);
        assertThat(result, equalTo("A=A#B=B#C=C"));
    }

    @Test
    public void test_Join_on_AppendTo_file() {
        try (FileWriter fw = new FileWriter(new File("E:/guava-test.txt"))) {
            Map map = new HashMap();
            map.put("A", "A");
            map.put("B", "B");
            map.put("C", "C");
            Joiner.on("#").withKeyValueSeparator("=").appendTo(fw, map);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
