package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * describe:
 *
 * 没搞清楚有什么作用？？？？？？？？？？？？？？？？？？？？？？？？
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class CombinableGroupReduceFunctionExample {
    public static void main(String[] args) throws Exception{
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

        input.groupBy(0)
                .reduceGroup(new MyCombinableGroupReduceFunction())
                .print();


    }

    public static class MyCombinableGroupReduceFunction implements GroupReduceFunction<Tuple3<String, Integer, Long>,Tuple3<String, Integer,Integer>>
            ,GroupCombineFunction<Tuple3<String, Integer, Long>,Tuple3<String, Integer,Integer>>{
        @Override
        public void combine(Iterable<Tuple3<String, Integer, Long>> values
                , Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            String key = null;
            int count = 0;
            int sum = 0;
            for(Tuple3<String, Integer, Long> value:values){
                if (key == null) {
                    key = value.f0;
                }
                count += 1;
                sum += value.f1;
                Optional.ofNullable("Thread id:"+Thread.currentThread().getId()+",Combine funcation process: "+value).ifPresent(System.out::println);
            }
            out.collect(Tuple3.of(key, count, sum));
        }

        @Override
        public void reduce(Iterable<Tuple3<String, Integer, Long>> values
                , Collector<Tuple3<String, Integer,Integer>> out) throws Exception {
            String key = null;
            int count = 0;
            int sum = 0;
            for(Tuple3<String, Integer, Long> value:values){
                if (key == null) {
                    key = value.f0;
                }
                count += 1;
                sum += value.f1;
                Optional.ofNullable("Thread id:"+Thread.currentThread().getId()+",Reduce funcation process: "+value).ifPresent(System.out::println);
            }
            out.collect(Tuple3.of(key, count, sum));
        }
    }
}

