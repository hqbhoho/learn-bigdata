package com.hqbhoho.bigdata.learnFlink.batch.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * describe:
 * <p>
 * Flink Batch Example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/18
 */
public class QuickStartExample {
    public static void main(String[] args) throws Exception {
        //构建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //模拟数据源
        DataSource<String> input = env.fromElements(
                "hello world",
                "hello flink",
                "spark streaming"
        );
        input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
