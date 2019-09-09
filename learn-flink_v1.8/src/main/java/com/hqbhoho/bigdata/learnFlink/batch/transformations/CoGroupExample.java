package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * describe:
 * <p>
 * 可以操作group by key之后，同一个key下面的两个dataset的所有数据
 * <p>
 * Test :      output   ==>   (key,input1.f1.max,input2.f1.max,timestamp)
 * (A,300,3,1557109591000)
 * (A,200,2,1557109593000)
 * (B,600,6,1557109593000)
 * (C,400,4,1557109599000)
 * (B,700,7,1557109594000)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/21
 */
public class CoGroupExample {
    public static void main(String[] args) throws Exception {
        // 创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 模拟数据源
        DataSource<Tuple3<String, Integer, Long>> input1 = env.fromElements(
                Tuple3.of("A", 300, 1557109591000L),
                Tuple3.of("A", 100, 1557109591000L),
                Tuple3.of("A", 200, 1557109593000L),
                Tuple3.of("B", 600, 1557109593000L),
                Tuple3.of("B", 700, 1557109594000L),
                Tuple3.of("B", 500, 1557109594000L),
                Tuple3.of("C", 400, 1557109599000L),
                Tuple3.of("C", 300, 1557109599000L)
        );
        DataSource<Tuple3<String, Integer, Long>> input2 = env.fromElements(
                Tuple3.of("A", 3, 1557109591000L),
                Tuple3.of("A", 1, 1557109591000L),
                Tuple3.of("A", 2, 1557109593000L),
                Tuple3.of("B", 6, 1557109593000L),
                Tuple3.of("B", 7, 1557109594000L),
                Tuple3.of("B", 5, 1557109594000L),
                Tuple3.of("C", 4, 1557109599000L),
                Tuple3.of("C", 3, 1557109599000L)
        );

        input1.coGroup(input2)
                .where(0, 2)
                .equalTo(0, 2)
                .with(new CoGroupFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Long>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, Integer, Long>> first, Iterable<Tuple3<String, Integer, Long>> second, Collector<Tuple4<String, Integer, Integer, Long>> out) throws Exception {
                        int max_1 = 0;
                        int max_2 = 0;
                        Long timeStamp = 0L;
                        String key = null;
                        for (Tuple3<String, Integer, Long> value : first) {
                            if (key == null) {
                                key = value.f0;
                            }
                            if (timeStamp == 0) {
                                timeStamp = value.f2;
                            }
                            max_1 = Math.max(max_1, value.f1);
                        }
                        for (Tuple3<String, Integer, Long> value : second) {
                            max_2 = Math.max(max_2, value.f1);
                        }
                        out.collect(Tuple4.of(key, max_1, max_2, timeStamp));
                    }
                })
                .print();

    }
}
