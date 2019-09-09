package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * describe:
 * <p>
 * A Reduce transformation that is applied on a grouped DataSet reduces each group to a single element using a user-defined reduce function.
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class ReduceOnDataSetExample {
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
                Tuple3.of("B", 600, 1557109595000L),
                Tuple3.of("B", 700, 1557109594000L),
                Tuple3.of("B", 500, 1557109596000L),
                Tuple3.of("C", 400, 1557109599000L),
                Tuple3.of("C", 300, 1557109598000L)
        );
        // Reduce On Grouped DataSet
        // 可以多个字段进行group by
        input.groupBy(0, 2)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> value1, Tuple3<String, Integer, Long> value2) throws Exception {
                        return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2);
                    }
                })
                .print();
        // Reduce On Full DataSet
        input.reduce(new ReduceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> value1, Tuple3<String, Integer, Long> value2) throws Exception {
                return Tuple3.of("", value1.f1 + value2.f1, Math.max(value1.f2, value2.f2));
            }
        }).print();

    }
}
