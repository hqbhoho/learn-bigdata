package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * 类似sql中的count(distinct a)
 * Distinct can be applied to GroupBy Aggregation, GroupBy Window Aggregation and Over Window Aggregation.
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/30
 */
public class DistinctAggregationExample {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 模拟数据源
        DataStreamSource<Tuple4<String, Integer, String, Integer>> input = env.fromElements(
                Tuple4.of("hqbhoho01", 21, "male", 10000),
                Tuple4.of("hqbhoho01", 22, "male", 20000),
                Tuple4.of("hqbhoho01", 21, "male", 30000),
                Tuple4.of("hqbhoho01", 21, "male", 40000)
        );

        Table inputTable = tableEnv.fromDataStream(input, "name,age,gender,account");
        Table resultTable = inputTable.groupBy("name")
                .select("name,age.count.distinct as a");

        tableEnv.toRetractStream(resultTable, Row.class)
                .print();

        env.execute("DistinctAggregationExample");

    }
}
