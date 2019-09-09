package com.hqbhoho.bigdata.learnFlink.table_sql.sql;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * {@link com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI.GroupByAggregationExample}
 * <p>
 * sql GroupByAggregation
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/04
 */
public class GroupByAggregationExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple4<String, Integer, String, Integer>> input = env.fromElements(
                Tuple4.of("hqbhoho01", 21, "male", 10000),
                Tuple4.of("hqbhoho02", 22, "male", 20000),
                Tuple4.of("hqbhoho03", 23, "male", 30000),
                Tuple4.of("hqbhoho04", 24, "male", 40000),
                Tuple4.of("hqbhoho04", 24, "male", 50000)
        );

        Table inputTable = tableEnv.fromDataStream(input, "name,age,gender,account");

        Table resultTable = tableEnv.sqlQuery("select name,sum(account) from " + inputTable + " group by name");

        tableEnv.toRetractStream(resultTable, Row.class)
                .print();

        env.execute("GroupByAggregationExample");
    }
}
