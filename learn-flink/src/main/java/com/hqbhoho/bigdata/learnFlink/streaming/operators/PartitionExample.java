package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/**
 * describe:
 * <p>
 * event 在Operator之间的分发方式
 * dataStream.shuffle() 随机分发
 * dataStream.rebalance() 轮询分发
 * dataStream.rescale()  数据本地轮询分发    减少taskmanager之间的IO传输
 * dataStream.broadcast() 广播分发
 * dataStream.forward() 上下算子一对一
 * dataStream.partitionCustom(partitioner, "someKey") 自定义分发规则
 * <p>
 * Test 自定义分发规则:
 * <p>
 * 当前处理线程：57,处理event：[xiaomixiu,500]
 * 当前处理线程：57,处理event：[xiaomixiu001,500]
 * 当前处理线程：56,处理event：[hqbhoho,100]
 * 当前处理线程：56,处理event：[hqb,500]
 * 当前处理线程：56,处理event：[hqb001,500]
 * 当前处理线程：56,处理event：[hqb002,500]
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/13
 */
public class PartitionExample {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStreamSource<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("hqbhoho", 100),
                Tuple2.of("xiaomixiu", 500),
                Tuple2.of("hqb", 500),
                Tuple2.of("hqb001", 500),
                Tuple2.of("hqb002", 500),
                Tuple2.of("xiaomixiu001", 500)

        );
        input.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                if (key.contains("h")) {
                    return 1;
                } else {
                    return 2;
                }
            }
        }, 0).map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                Optional.ofNullable("当前处理线程：" + Thread.currentThread().getId() + ",处理event：[" + value.f0 + "," + value.f1 + "]")
                        .ifPresent(System.out::println);
                return "1";
            }

        }).print();
        env.execute("PartitionExample");
    }
}
