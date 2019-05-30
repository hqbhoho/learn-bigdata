package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * 过滤操作
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/29
 */
public class FilterOperationExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple4<String, Integer, String, Integer>> input = env.fromElements(
                Tuple4.of("hqbhoho01", 21, "male", 10000),
                Tuple4.of("hqbhoho02", 22, "male", 20000),
                Tuple4.of("hqbhoho03", 23, "male", 30000),
                Tuple4.of("hqbhoho04", 24, "male", 40000)
        );

        tableEnv.registerDataStream("example", input);

        Table result = tableEnv.scan("example")
                .as("name,age,sex,account")
                .filter("account >= 20000")
                .where("age >= 23");

        tableEnv.toAppendStream(result, TypeInformation.of(new TypeHint<Row>() {
        }))
                .print();
        env.execute("FilterOperationExample");
    }
}
