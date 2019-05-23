package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * describe:
 * <p>
 * similar as sql union
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/21
 */
public class UnionExample {
    public static void main(String[] args) throws Exception {
        // 创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 模拟数据源
        DataSource<Tuple3<String, Integer, Long>> input1 = env.fromElements(
                Tuple3.of("A", 300, 1557109591000L),
                Tuple3.of("A", 100, 1557109592000L),
                Tuple3.of("A", 200, 1557109593000L),
                Tuple3.of("B", 600, 1557109595000L),
                Tuple3.of("B", 700, 1557109594000L),
                Tuple3.of("B", 500, 1557109596000L),
                Tuple3.of("C", 400, 1557109599000L),
                Tuple3.of("C", 300, 1557109598000L)
        );
        DataSource<Tuple3<String, Integer, Long>> input2 = env.fromElements(
                Tuple3.of("A", 3, 1557109591000L),
                Tuple3.of("A", 1, 1557109592000L),
                Tuple3.of("A", 2, 1557109593000L),
                Tuple3.of("B", 6, 1557109595000L),
                Tuple3.of("B", 7, 1557109594000L),
                Tuple3.of("B", 5, 1557109596000L),
                Tuple3.of("C", 4, 1557109599000L),
                Tuple3.of("C", 3, 1557109598000L)
        );

        input1.union(input2).print();
    }
}
