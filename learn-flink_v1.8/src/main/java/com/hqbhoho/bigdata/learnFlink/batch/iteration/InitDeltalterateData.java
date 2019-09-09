package com.hqbhoho.bigdata.learnFlink.batch.iteration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * describe:
 * <p>
 * 增量迭代准备初始数据
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/22
 */
public class InitDeltalterateData {

    // 顶点编号
    public static final List<Long> VERTICES = Arrays.asList(/*0L,*/1L, 2L, 3L, 4L, 5L, 6L, 7L);
    // 顶点之间的边连接信息
    public static final List<Tuple2<Long, Long>> EDGES = Arrays.asList(
            Tuple2.of(1L, 2L),
            Tuple2.of(2L, 3L),
            Tuple2.of(2L, 4L),
            Tuple2.of(3L, 4L),
//            Tuple2.of(3L, 0L),
            Tuple2.of(5L, 6L),
            Tuple2.of(5L, 7L),
            Tuple2.of(6L, 7L)
    );

    // 初始化时,每个顶点都认为自己是最小的值
    public static DataSet<Tuple2<Long, Long>> initialSolutionSet(ExecutionEnvironment env) {
        return env.fromCollection(VERTICES).map(new MapFunction<Long, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Long value) throws Exception {
                return Tuple2.of(value, value);
            }
        });
    }

    // 获取每个顶点的所有邻居节点的关系
    public static DataSet<Tuple2<Long, Long>> initialEdges(ExecutionEnvironment env) {
        return env.fromCollection(EDGES).flatMap(new FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            @Override
            public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
                out.collect(Tuple2.of(value.f0, value.f1));
                out.collect(Tuple2.of(value.f1, value.f0));
            }
        });
    }
}
