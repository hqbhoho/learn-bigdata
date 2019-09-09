package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * describe:
 *
 * join操作（下文已inner join做测试，outer join大部分功能同inner join,具体可以参见官网）
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class JoinExample {
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
        // JoinFunction()
        /*input1.join(input2)
                .where(0, 2)
                .equalTo(0, 2)
                .with(new JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Long>>() {
                    @Override
                    public Tuple4<String, Integer, Integer, Long> join(Tuple3<String, Integer, Long> first, Tuple3<String, Integer, Long> second) throws Exception {
                        Optional.ofNullable("Thread ID: "+Thread.currentThread().getId()+",process event: "+first+","+second)
                                .ifPresent(System.out::println);
                        return Tuple4.of(first.f0, first.f1, second.f1, first.f2);
                    }
                })
                .print();*/
        // project()
        /*input1.join(input2)
                .where(0,2)
                .equalTo(0,2)
                .projectFirst(0,1).projectSecond(1).projectFirst(2)
                .print();*/
        // FlatJoinFunction
        input1.join(input2)
                .where(0, 2)
                .equalTo(0, 2)
                .with(new FlatJoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Long>>() {
                    @Override
                    public void join(Tuple3<String, Integer, Long> first, Tuple3<String, Integer, Long> second, Collector<Tuple4<String, Integer, Integer, Long>> out) throws Exception {
                        if (!first.f0.equalsIgnoreCase("A")) {
                            out.collect(Tuple4.of(first.f0, first.f1, second.f1, first.f2));
                        }
                    }
                })
                .print();
    }
}
