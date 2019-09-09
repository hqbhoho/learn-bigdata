package com.hqbhoho.bigdata.guava.utils;

import com.google.common.base.Preconditions;
import org.junit.Test;

import java.util.Optional;

/**
 * describe:
 * <p>
 * 参数检查
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/05
 */
public class TestPreconditions {

    @Test
    public void test_check_nullValue() {
        try {
//            Preconditions.checkArgument(null == null, "is null");
//            Preconditions.checkNotNull(null,"is null...%s","haha");
            Preconditions.checkState("" == null, "is not null");
        } catch (Exception e) {
            Optional.ofNullable(e.getMessage()).ifPresent(System.out::println);
        }
    }

    @Test
    public void test_check_valid_potision() {
        try {
//            Preconditions.checkElementIndex(2, 2, "checkElementIndex [0,2)");
            Preconditions.checkPositionIndex(2, 2, "checkPositionIndex [0,2]");
        } catch (Exception e) {
            Optional.ofNullable(e.getMessage()).ifPresent(System.out::println);
        }
    }

    @Test
    public void test_assert(){
        try {
            assert "" == null : "is not null";
            Optional.ofNullable("I am invoked......").ifPresent(System.out::println);
        }catch(Error e){
            Optional.ofNullable(e.getMessage()).ifPresent(System.out::println);
        }
    }

}
