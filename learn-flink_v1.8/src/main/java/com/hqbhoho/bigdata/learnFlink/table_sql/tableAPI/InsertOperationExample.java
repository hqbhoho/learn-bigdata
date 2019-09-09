package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

/**
 * describe:
 * <p>
 * sql :   insert into
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/03
 */
public class InsertOperationExample {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 模拟数据源
        DataStreamSource<Tuple4<String, Integer, String, Integer>> input = env.fromElements(
                Tuple4.of("hqbhoho", 21, "male", 30000),
                Tuple4.of("xiaomixiu", 22, "male", 20000),
                Tuple4.of("hqbhoho", 23, "male", 10000),
                Tuple4.of("haibo", 24, "male", 50000),
                Tuple4.of("xiaomixiu", 24, "male", 40000)
        );

        // DataStream ---> Table
        Table inputTable = tableEnv.fromDataStream(input, "name,age,gender,account")
                .select("name,account");
        tableEnv.registerTableSink("results",
                new String[]{"name", "account"},
                new TypeInformation[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO
                },
                new CsvTableSink("E:\\javaProjects\\learn-bigdata\\learn-flink\\src\\main\\resources\\results.csv"
                , ",", 1, FileSystem.WriteMode.NO_OVERWRITE));

        inputTable.insertInto("results");
        env.execute("InsertOperationExample");
    }
}
