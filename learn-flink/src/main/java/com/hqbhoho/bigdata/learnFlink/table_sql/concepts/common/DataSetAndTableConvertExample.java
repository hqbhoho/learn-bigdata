package com.hqbhoho.bigdata.learnFlink.table_sql.concepts.common;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * convert a DataSet into a Table and vice versa
 * <p>
 * DataSet  --->  Table
 * 1.Register a DataSet as Table
 * 2.Convert a DataSet into a Table
 * Table   ---> DataSet
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/27
 */
public class DataSetAndTableConvertExample {
    public static void main(String[] args) throws Exception {
        // 创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        // 模拟数据源
        DataSource<Tuple3<String, Integer, Long>> input = env.fromElements(
                Tuple3.of("A", 300, 1557109591000L),
                Tuple3.of("A", 100, 1557109592000L),
                Tuple3.of("A", 200, 1557109593000L),
                Tuple3.of("B", 600, 1557109595000L),
                Tuple3.of("B", 700, 1557109594000L),
                Tuple3.of("B", 500, 1557109596000L),
                Tuple3.of("C", 400, 1557109599000L),
                Tuple3.of("C", 300, 1557109598000L)
        );
        /*DataSet  --->  Table*/
        //Register a DataSet as Table
        tableEnv.registerDataSet(
                "Register-DataSet-table",
                input,
                "name,account,timestamp");
        // Convert a DataSet into a Table
        Table table = tableEnv.fromDataSet(input, "name,account");

        // data process
        Table result1 = tableEnv.scan("Register-DataSet-table")
                .groupBy("name")
                .select("name,account.sum as sum");
        Table result2 = table.groupBy("name")
                .select("name,account.sum as sum");

        /* Table ---> DataSet*/
        tableEnv.toDataSet(result1, TypeInformation.of(new TypeHint<Row>() {
        })).print();
        tableEnv.toDataSet(result2, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        })).print();

    }
}
