package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * describe:
 * <p>
 * 数据路由方式
 * **********************************************************************************
 * 默认值(未设置下游算子并行度)：  多并行度同rebalance
 * Thread ID: 58,process event: (A,100,1557109592000)
 * Thread ID: 58,process event: (G,400,1557109590000)
 * Thread ID: 58,process event: (B,500,1557109596000)
 * Thread ID: 60,process event: (A,300,1557109591000)
 * Thread ID: 60,process event: (F,400,1557109590000)
 * Thread ID: 60,process event: (B,700,1557109594000)
 * Thread ID: 59,process event: (D,300,1557109597000)
 * Thread ID: 59,process event: (B,600,1557109595000)
 * Thread ID: 59,process event: (C,300,1557109598000)
 * Thread ID: 62,process event: (A,200,1557109593000)
 * Thread ID: 62,process event: (H,400,1557109590000)
 * Thread ID: 62,process event: (C,400,1557109599000)
 * **********************************************************************************
 * Rebalance         实现多并行度下的负载均衡
 * input.rebalance()
 * <p>
 * Thread ID: 59,process event: (hqbhoho,100,1557109592000)
 * Thread ID: 59,process event: (xiaomixiu,500,1557109596000)
 * Thread ID: 59,process event: (hfgl,400,1557109590000)
 * Thread ID: 60,process event: (hqbhoho,200,1557109593000)
 * Thread ID: 60,process event: (ggwp,400,1557109599000)
 * Thread ID: 58,process event: (hqbhoho,300,1557109591000)
 * Thread ID: 58,process event: (xiaomixiu,700,1557109594000)
 * Thread ID: 58,process event: (hfgl,300,1557109597000)
 * Thread ID: 61,process event: (xiaomixiu,600,1557109595000)
 * Thread ID: 61,process event: (ggwp,300,1557109598000)
 * **********************************************************************************
 * Hash-Partition  根据key的hash值进行分区
 * input.partitionByHash(0)
 * <p>
 * Thread ID: 59,process event: (A,300,1557109591000)
 * Thread ID: 59,process event: (A,100,1557109592000)
 * Thread ID: 59,process event: (A,200,1557109593000)
 * Thread ID: 59,process event: (F,400,1557109590000)
 * Thread ID: 60,process event: (D,300,1557109597000)
 * Thread ID: 60,process event: (H,400,1557109590000)
 * Thread ID: 60,process event: (B,600,1557109595000)
 * Thread ID: 60,process event: (B,700,1557109594000)
 * Thread ID: 60,process event: (B,500,1557109596000)
 * Thread ID: 60,process event: (C,400,1557109599000)
 * Thread ID: 60,process event: (C,300,1557109598000)
 * Thread ID: 62,process event: (G,400,1557109590000)
 * **********************************************************************************
 * Range-Partition 将一定范围内的key(有序)映射到某一个分区内
 * input.partitionByRange(0)
 * <p>
 * Thread ID: 65,process event: (D,300,1557109597000)
 * Thread ID: 65,process event: (F,400,1557109590000)
 * Thread ID: 67,process event: (C,400,1557109599000)
 * Thread ID: 67,process event: (C,300,1557109598000)
 * Thread ID: 64,process event: (A,300,1557109591000)
 * Thread ID: 64,process event: (A,100,1557109592000)
 * Thread ID: 64,process event: (A,200,1557109593000)
 * Thread ID: 64,process event: (B,600,1557109595000)
 * Thread ID: 64,process event: (B,700,1557109594000)
 * Thread ID: 64,process event: (B,500,1557109596000)
 * Thread ID: 68,process event: (G,400,1557109590000)
 * Thread ID: 68,process event: (H,400,1557109590000)
 * <p>
 * input.partitionByRange(0).withOrders(Order.DESCENDING)   only range can use 对key先排序  在分区
 * <p>
 * Thread ID: 67,process event: (A,300,1557109591000)
 * Thread ID: 67,process event: (A,100,1557109592000)
 * Thread ID: 67,process event: (A,200,1557109593000)
 * Thread ID: 65,process event: (D,300,1557109597000)
 * Thread ID: 65,process event: (F,400,1557109590000)
 * Thread ID: 65,process event: (G,400,1557109590000)
 * Thread ID: 65,process event: (H,400,1557109590000)
 * Thread ID: 68,process event: (B,600,1557109595000)
 * Thread ID: 68,process event: (B,700,1557109594000)
 * Thread ID: 68,process event: (B,500,1557109596000)
 * Thread ID: 68,process event: (C,400,1557109599000)
 * Thread ID: 68,process event: (C,300,1557109598000)
 * <p>
 * input.partitionByRange(0).withOrders(Order.ASCENDING)
 * <p>
 * Thread ID: 64,process event: (C,400,1557109599000)
 * Thread ID: 64,process event: (C,300,1557109598000)
 * Thread ID: 66,process event: (A,300,1557109591000)
 * Thread ID: 66,process event: (A,100,1557109592000)
 * Thread ID: 66,process event: (A,200,1557109593000)
 * Thread ID: 66,process event: (B,600,1557109595000)
 * Thread ID: 66,process event: (B,700,1557109594000)
 * Thread ID: 66,process event: (B,500,1557109596000)
 * Thread ID: 68,process event: (G,400,1557109590000)
 * Thread ID: 68,process event: (H,400,1557109590000)
 * Thread ID: 67,process event: (D,300,1557109597000)
 * Thread ID: 67,process event: (F,400,1557109590000)
 * ********************************************************************************************************
 * Sort Partition  分区内排序
 * input.partitionByHash(0).sortPartition(1, Order.ASCENDING).sortPartition(2, Order.DESCENDING)
 * Thread ID: 76,process event: (G,400,1557109590000)
 * Thread ID: 75,process event: (A,100,1557109592000)
 * Thread ID: 75,process event: (A,200,1557109593000)
 * Thread ID: 75,process event: (A,300,1557109591000)
 * Thread ID: 75,process event: (F,400,1557109590000)
 * Thread ID: 79,process event: (C,300,1557109598000)
 * Thread ID: 79,process event: (D,300,1557109597000)
 * Thread ID: 79,process event: (C,400,1557109599000)
 * Thread ID: 79,process event: (H,400,1557109590000)
 * Thread ID: 79,process event: (B,500,1557109596000)
 * Thread ID: 79,process event: (B,600,1557109595000)
 * Thread ID: 79,process event: (B,700,1557109594000)
 * *********************************************************************************************************
 * partitionConsumer
 * input.partitionCustom(new MyPartitioner(), 0).sortPartition(1, Order.ASCENDING).sortPartition(2, Order.DESCENDING)
 * (A,B),(other)
 * Thread ID: 76,process event: (A,100,1557109592000)
 * Thread ID: 76,process event: (A,200,1557109593000)
 * Thread ID: 76,process event: (A,300,1557109591000)
 * Thread ID: 76,process event: (B,500,1557109596000)
 * Thread ID: 76,process event: (B,600,1557109595000)
 * Thread ID: 76,process event: (B,700,1557109594000)
 * Thread ID: 79,process event: (C,300,1557109598000)
 * Thread ID: 79,process event: (D,300,1557109597000)
 * Thread ID: 79,process event: (C,400,1557109599000)
 * Thread ID: 79,process event: (F,400,1557109590000)
 * Thread ID: 79,process event: (G,400,1557109590000)
 * Thread ID: 79,process event: (H,400,1557109590000)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class PartitionExample {

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

        input.partitionCustom(new MyPartitioner(), 0)
                .sortPartition(1, Order.ASCENDING)
                .sortPartition(2, Order.DESCENDING)
                .mapPartition(new MapPartitionFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple3<String, Integer, Long>> values, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
                        values.forEach(value -> {
                            Optional.ofNullable("Thread ID: " + Thread.currentThread().getId() + ",process event: " + value)
                                    .ifPresent(System.out::println);
                        });
                    }
                }).print();
    }

    /**
     * customer partitioner
     */
    static class MyPartitioner implements Partitioner<String> {

        @Override
        public int partition(String key, int numPartitions) {
            if (key.contains("A") || key.contains("B")) {
                return 1;
            } else {
                return 2;
            }
        }
    }
}
