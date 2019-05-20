package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Optional;

/**
 * describe:
 * <p>
 * 有点类似 MapReduce中map端的提前Combine   减少数据在网络间的传输
 * 尚不是很清楚其于reduceGroup算子的区别?????????????????????????
 * GroupCombineOnGroupedDataSetExample
 * <p>
 * Combine funcation process: (A,300,1557109591000)
 * Combine funcation process: (A,100,1557109592000)
 * Combine funcation process: (A,200,1557109593000)
 * Combine funcation process: (B,600,1557109595000)
 * Combine funcation process: (B,700,1557109594000)
 * Combine funcation process: (B,500,1557109596000)
 * Combine funcation process: (C,400,1557109599000)
 * Combine funcation process: (C,300,1557109598000)
 * Combine funcation process: (D,300,1557109597000)
 * Combine funcation process: (F,400,1557109590000)
 * Combine funcation process: (G,400,1557109590000)
 * Combine funcation process: (H,400,1557109590000)
 * (12,28800)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class GroupCombineOnDataSetExample {
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

        // GroupCombine On Grouped DataSet
        input.groupBy(0)
                .combineGroup(new CombinableGroupReduceFunctionExample.MyCombinableGroupReduceFunction())
                .<Tuple2<Integer, Integer>>project(1, 2)
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        Optional.ofNullable("reduce process event value1: "+ value1).ifPresent(System.out::println);
                        Optional.ofNullable("reduce process event value2: "+ value2).ifPresent(System.out::println);
                        return Tuple2.of(value1.f0 + value2.f0, value1.f1 + value2.f1);
                    }
                })
                .print();
        // GroupCombine On Full DataSet
        input.rebalance().combineGroup(new CombinableGroupReduceFunctionExample.MyCombinableGroupReduceFunction())
                .print();
    }

}
