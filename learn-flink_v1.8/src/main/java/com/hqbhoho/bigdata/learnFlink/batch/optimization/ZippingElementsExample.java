package com.hqbhoho.bigdata.learnFlink.batch.optimization;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Optional;

/**
 * describe:
 * <p>
 * assign unique identifiers to data set elements
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/23
 */
public class ZippingElementsExample {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataSource<Long> input = env.fromParallelCollection(new NumberSequenceIterator(11L,20L), BasicTypeInfo.LONG_TYPE_INFO);
        // first counting then labeling elements
       /* DataSetUtils.zipWithIndex(input).filter(new FilterFunction<Tuple2<Long, Long>>() {
            @Override
            public boolean filter(Tuple2<Long, Long> value) throws Exception {
                Optional.of("Thread ID: "+Thread.currentThread().getId()+",process event: "+value)
                        .ifPresent(System.out::println);
                return value.f0 >= 3;
            }
        }).print();*/

        DataSetUtils.zipWithUniqueId(input).filter(new FilterFunction<Tuple2<Long, Long>>() {
            @Override
            public boolean filter(Tuple2<Long, Long> value) throws Exception {
                Optional.of("Thread ID: " + Thread.currentThread().getId() + ",process event: " + value)
                        .ifPresent(System.out::println);
                return value.f0 >= 3;
            }
        }).print();
    }
}
