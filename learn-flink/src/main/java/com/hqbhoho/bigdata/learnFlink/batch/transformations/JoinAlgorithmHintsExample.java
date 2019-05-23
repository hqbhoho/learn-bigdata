package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Optional;

/**
 * describe:
 * <p>
 * 为join操作选择一个合适的join策略（下文已inner join做测试，outer join大部分功能同inner join,具体可以参见官网）
 * refence：
 * https://flink.apache.org/news/2015/03/13/peeking-into-Apache-Flinks-Engine-Room.html
 * <p>
 * OPTIMIZER_CHOOSES:         Flink优化器内部自行决定使用何种join策略
 * BROADCAST_HASH_FIRST:     广播第一个dataset，并建立哈希表。第二个dataset去和哈希表join    适合第一个DataSet小的场景
 * BROADCAST_HASH_SECOND:    参上
 * REPARTITION_HASH_FIRST:   两个dataset都很大，根据join的key使用相同的分区规则，先分区，分区后，根据第一个dataset分区数据，建立哈希表，第二个dataset分区数据去和哈希表join 适合两个大的dataset,第一个会小于第二个
 * REPARTITION_HASH_SECOND:  参上
 * REPARTITION_SORT_MERGE:   两个dataset都很大，根据join的key使用相同的分区规则，先分区，分区后，两个dataset的数据进行先根据key排序，然后join,适合两个大的有序dataset
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/20
 */
public class JoinAlgorithmHintsExample {
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

        // Join with DataSet Size Hint
        //  Hint input2 is small   source code: new JoinOperatorSets<>(this, other, JoinHint.BROADCAST_HASH_SECOND);

        /*input1.joinWithTiny(input2)
                .where(0, 2)
                .equalTo(0, 2)
                .projectFirst(0, 1).projectSecond(1).projectFirst(2)
                .print();*/

        // Hint input2 is big    source code:  new JoinOperatorSets<>(this, other, JoinHint.BROADCAST_HASH_FIRST)

        /*input1.joinWithHuge(input2)
                .where(0, 2)
                .equalTo(0, 2)
                .projectFirst(0, 1).projectSecond(1).projectFirst(2)
                .print();*/

        // BROADCAST_HASH_FIRST
        /*
          [Join (Join at main(JoinAlgorithmHintsExample.java:81)) (4/4)]  join操作并行度：4
          Thread ID: 56,process first: (A,300,1557109591000),second: (A,3,1557109591000)
          Thread ID: 56,process first: (A,100,1557109592000),second: (A,1,1557109592000)
          Thread ID: 56,process first: (A,200,1557109593000),second: (A,2,1557109593000)
          Thread ID: 58,process first: (B,600,1557109595000),second: (B,6,1557109595000)
          Thread ID: 58,process first: (B,700,1557109594000),second: (B,7,1557109594000)
          Thread ID: 58,process first: (B,500,1557109596000),second: (B,5,1557109596000)
          Thread ID: 58,process first: (C,400,1557109599000),second: (C,4,1557109599000)
          Thread ID: 58,process first: (C,300,1557109598000),second: (C,3,1557109598000)
          input1整体 被广播到input2中的partition中进行join
         */

        /*input1.join(input2.partitionByHash(0), JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
                .where(0, 2)
                .equalTo(0, 2)
                .with(new JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Long>>() {
                    @Override
                    public Tuple4<String, Integer, Integer, Long> join(Tuple3<String, Integer, Long> first, Tuple3<String, Integer, Long> second) throws Exception {
                        Optional.of("Thread ID: " + Thread.currentThread().getId() + ",process first: " + first + ",second: " + second)
                                .ifPresent(System.out::println);
                        return Tuple4.of(first.f0, first.f1, second.f1, first.f2);
                    }
                }).print();*/

        // REPARTITION_HASH_FIRST
        /*
           [Join (Join at main(JoinAlgorithmHintsExample.java:130)) (4/4)]  join操作并行度：4
           Thread ID: 59,process first: (A,100,1557109592000),second: (A,1,1557109592000)
           Thread ID: 59,process first: (B,500,1557109596000),second: (B,5,1557109596000)
           Thread ID: 59,process first: (C,400,1557109599000),second: (C,4,1557109599000)
           Thread ID: 59,process first: (C,300,1557109598000),second: (C,3,1557109598000)
           Thread ID: 61,process first: (B,700,1557109594000),second: (B,7,1557109594000)
           Thread ID: 55,process first: (A,300,1557109591000),second: (A,3,1557109591000)
           Thread ID: 55,process first: (A,200,1557109593000),second: (A,2,1557109593000)
           Thread ID: 55,process first: (B,600,1557109595000),second: (B,6,1557109595000)
           两个dataset根据join key先进行repartition,之后分区数据进行join
        */

        input1.join(input2.partitionByHash(0), JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST)
                .where(0,2)
                .equalTo(0,2)
                .with(new JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Long>>() {
                    @Override
                    public Tuple4<String, Integer, Integer, Long> join(Tuple3<String, Integer, Long> first, Tuple3<String, Integer, Long> second) throws Exception {
                        Optional.of("Thread ID: " + Thread.currentThread().getId() + ",process first: " + first + ",second: " + second)
                                .ifPresent(System.out::println);
                        return Tuple4.of(first.f0, first.f1, second.f1, first.f2);
                    }
                }).print();

    }
}
