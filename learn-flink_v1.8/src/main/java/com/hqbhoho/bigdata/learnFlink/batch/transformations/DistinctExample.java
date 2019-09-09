package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * describe:
 * <p>
 * 根据key 去重   保留的是第一个元素   去除后续重复的元素
 *
 * (A,300,1557109591000)
 * (B,600,1557109592000)
 * (C,400,1557109599000)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class DistinctExample {
    public static void main(String[] args) throws Exception {
        //构建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //模拟数据源
        DataSource<Tuple3<String, Integer, Long>> input = env.fromElements(
                Tuple3.of("A", 300, 1557109591000L),
                Tuple3.of("A", 100, 1557109591000L),
                Tuple3.of("A", 200, 1557109591000L),
                Tuple3.of("B", 600, 1557109592000L),
                Tuple3.of("B", 700, 1557109592000L),
                Tuple3.of("B", 500, 1557109592000L),
                Tuple3.of("C", 400, 1557109599000L),
                Tuple3.of("C", 300, 1557109599000L)
        );

        input.distinct(0, 2)
                .print();
    }

}
