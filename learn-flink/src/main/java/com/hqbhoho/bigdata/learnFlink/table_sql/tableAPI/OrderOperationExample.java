package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * only Batch process
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/03
 */
public class OrderOperationExample {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        // 模拟数据源
        DataSource<Tuple4<String, Integer, String, Integer>> input = env.fromElements(
                Tuple4.of("hqbhoho", 21, "male", 30000),
                Tuple4.of("xiaomixiu", 22, "male", 20000),
                Tuple4.of("hqbhoho", 23, "male", 10000),
                Tuple4.of("haibo", 24, "male", 50000),
                Tuple4.of("xiaomixiu", 24, "male", 40000)
        );
        // DataSet ---> Table
        tableEnv.registerDataSet("example", input, "name,age,genger,account");

        Table resultTable = tableEnv.scan("example")
                .orderBy("name.asc,age.desc")
                .offset(1)
                .fetch(3);

        // Table ---> DataSet
        tableEnv.toDataSet(resultTable, Row.class)
                .print();

    }
}
