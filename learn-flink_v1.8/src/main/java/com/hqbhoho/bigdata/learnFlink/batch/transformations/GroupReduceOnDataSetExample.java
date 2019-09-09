package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Optional;

/**
 * describe:
 * <p>
 * The difference between this and Reduce is that the user defined function gets the whole group at once.
 * <p>
 * (A,3,600)
 * (F,1,400)
 * (B,3,1800)
 * (C,2,700)
 * (D,1,300)
 * (H,1,400)
 * (G,1,400)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class GroupReduceOnDataSetExample {
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

        // GroupReduce On Grouped DataSet
        input.groupBy(0)
                // 统计f0 出现次数count以及f1字段的sum值
                .reduceGroup(new GroupReduceFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<String, Integer, Long>> values, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        String key = null;
                        int count = 0;
                        int sum = 0;
                        Iterator<Tuple3<String, Integer, Long>> it = values.iterator();
                        while (it.hasNext()) {
                            Tuple3<String, Integer, Long> value = it.next();
                            if (key == null) {
                                key = value.f0;
                            }
                            count += 1;
                            sum += value.f1;
                        }
                        out.collect(Tuple3.of(key, count, sum));
                    }
                })
                .print();
        //  GroupReduce On Full DataSet
        // 普通的reduceGroup  处理dataset的所有数据
        input.reduceGroup(new GroupReduceFunction<Tuple3<String, Integer, Long>, Tuple2<Integer, Integer>>() {
            @Override
            public void reduce(Iterable<Tuple3<String, Integer, Long>> values, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                int count = 0;
                int sum = 0;
                Iterator<Tuple3<String, Integer, Long>> it = values.iterator();
                while (it.hasNext()) {
                    Tuple3<String, Integer, Long> value = it.next();
                    count += 1;
                    sum += value.f1;
                    Optional.ofNullable("Thread id:"+Thread.currentThread().getId()+",Reduce funcation process: "+value).ifPresent(System.out::println);
                }
                out.collect(Tuple2.of(count, sum));
            }
        }).setParallelism(4).print();
        //  A GroupReduce transformation on a full DataSet cannot be done in parallel if the group-reduce function is not combinable.
        // 没有测试出效果，待进一步研究测试
        input.reduceGroup(new CombinableGroupReduceFunctionExample.MyCombinableGroupReduceFunction()).setParallelism(4)
                .print();

    }
}
