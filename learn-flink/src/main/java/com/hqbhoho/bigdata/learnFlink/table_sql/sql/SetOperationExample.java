package com.hqbhoho.bigdata.learnFlink.table_sql.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * 以 in 为例
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/05
 */
public class SetOperationExample {
    public static void main(String[] args) throws Exception {
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 模拟数据源
        DataStreamSource<Tuple4<String, Integer, String, Integer>> input1 = env.fromElements(
                Tuple4.of("hqbhoho", 21, "male", 10000),
                Tuple4.of("xiaomixiu", 22, "male", 20000),
                Tuple4.of("mixiu", 23, "male", 30000),
                Tuple4.of("haibo", 24, "male", 40000),
                Tuple4.of("xiaohaibo", 24, "male", 50000)
        );
        DataStreamSource<Tuple2<String, String>> input2 = env.fromElements(
                Tuple2.of("hqbhoho", "teacher"),
                Tuple2.of("haibo", "student")
        );

        // DataStream ---> Table
        tableEnv.registerDataStream("input1", input1, "name,age,gender,account");
        tableEnv.registerDataStream("input2", input2, "name,profession");

        Table resultTable = tableEnv.sqlQuery("select name,age,gender,account from input1" +
                " where name in ( select name from input2 )");

        tableEnv.toRetractStream(resultTable, Row.class)
                .print();

        env.execute("SetOperationExample");

    }
}
