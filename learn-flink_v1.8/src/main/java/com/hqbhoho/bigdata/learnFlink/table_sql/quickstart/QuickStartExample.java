package com.hqbhoho.bigdata.learnFlink.table_sql.quickstart;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;


/**
 * describe:
 * <p>
 * Quick Start Example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/27
 */
public class QuickStartExample {
    public static void main(String[] args) throws Exception{
        // 创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
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
        // 创建tableEnv
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        // datset -> table
        tableEnv.registerDataSet("info",input,"name,account,timestamp");
        // table api
        Table result = tableEnv.scan("info")
                .select("name,account")
                .groupBy("name")
                .select("name,account.sum as account_sum ");
        // table --> dataset
        TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(
                BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.INT_TYPE_INFO
        );
        tableEnv.toDataSet(result,tupleType)
                .print();
    }
}
