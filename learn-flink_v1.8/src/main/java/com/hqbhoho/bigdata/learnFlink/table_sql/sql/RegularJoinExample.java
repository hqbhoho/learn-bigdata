package com.hqbhoho.bigdata.learnFlink.table_sql.sql;

import com.hqbhoho.bigdata.learnFlink.streaming.operators.WindowJoinExample;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * Currently, only equi-joins are supported
 * <p>
 * {@link com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI.RegularJoinExample}
 * Test: 测试用例参见tableAPI.RegularJoinExample
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/05
 */
public class RegularJoinExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        int port2 = tool.getInt("port2", 29999);
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取数据源
        DataStream<Tuple3<String, Integer, Long>> input1 = env.socketTextStream(host, port1)
                .map(new WindowJoinExample.TokenFunction());
//                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks());
        DataStream<Tuple3<String, Integer, Long>> input2 = env.socketTextStream(host, port2)
                .map(new WindowJoinExample.TokenFunction());
//                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks());

        Table table1 = tableEnv.fromDataStream(input1, "name1,account,time1");
        Table table2 = tableEnv.fromDataStream(input2, "name2,num,time2");

        Table resultTable = tableEnv.sqlQuery("select name1,account,num from " + table1 + " inner join " + table2 + " on name1 = name2 ");

        tableEnv.toRetractStream(resultTable, Row.class)
                .print();
        env.execute("RegularJoinExample");
    }
}
