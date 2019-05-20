package com.hqbhoho.bigdata.learnFlink.batch.transformations;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * describe:
 * <p>
 * 为join操作选择一个合适的join策略（下文已inner join做测试，outer join大部分功能同inner join,具体可以参见官网）
 * <p>
 * OPTIMIZER_CHOOSES:     Flink优化器内部自行决定使用何种join策略
 * BROADCAST_HASH_FIRST:
 * BROADCAST_HASH_SECOND:
 * REPARTITION_HASH_FIRST:
 * REPARTITION_HASH_SECOND:
 * REPARTITION_SORT_MERGE:
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


    }
}
