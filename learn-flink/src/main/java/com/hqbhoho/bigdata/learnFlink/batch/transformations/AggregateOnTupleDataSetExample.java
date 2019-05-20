package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * describe:
 * <p>
 * only use with tuple
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class AggregateOnTupleDataSetExample {
    public static void main(String[] args) throws Exception {
        //构建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //模拟数据源
        DataSource<Tuple3<String, Integer, Long>> input = env.fromElements(
                Tuple3.of("A", 300, 1557109591000L),
                Tuple3.of("A", 100, 1557109592000L),
                Tuple3.of("A", 200, 1557109593000L),
                Tuple3.of("D", 300, 1557109597000L),
                Tuple3.of("F", 400, 1557109590000L),
                Tuple3.of("G", 400, 1557109590000L),
                Tuple3.of("H", 400, 1557109590000L),
                Tuple3.of("B", 600, 1557109591000L),
                Tuple3.of("B", 700, 1557109595000L),
                Tuple3.of("B", 500, 1557109598000L),
                Tuple3.of("C", 400, 1557109597000L),
                Tuple3.of("C", 300, 1557109598000L)
        );
        //  Aggregate On Grouped Tuple DataSet
        input.groupBy(0)
                .aggregate(Aggregations.SUM, 1)
                .andMax(2)
                .print();

        input.groupBy(0)
                .minBy(1,2)
                .print();

        // Aggregate On Full Tuple DataSet
        input.aggregate(Aggregations.SUM, 1)
                .andMax(2)
                .print();

        input.minBy(1, 2)
                .print();

    }
}
