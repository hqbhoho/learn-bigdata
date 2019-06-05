package com.hqbhoho.bigdata.learnFlink.table_sql.sql;

import com.hqbhoho.bigdata.learnFlink.streaming.operators.WindowJoinExample;
import com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.EventTimeAndWatermarkExample;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * {@link com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI.TimeWindowJoinExample}
 * Test: 测试用例参见tableAPI.TimeWindowJoinExample
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/05
 */
public class TimeWindowedJoinExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        int port2 = tool.getInt("port2", 29999);
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10000000);

        // 获取数据源
        DataStream<Tuple3<String, Integer, Long>> input1 = env.socketTextStream(host, port1)
                .map(new WindowJoinExample.TokenFunction())
                .assignTimestampsAndWatermarks(new EventTimeAndWatermarkExample.MyTimeExtractor());
        DataStream<Tuple3<String, Integer, Long>> input2 = env.socketTextStream(host, port2)
                .map(new WindowJoinExample.TokenFunction())
                .assignTimestampsAndWatermarks(new EventTimeAndWatermarkExample.MyTimeExtractor());

        // DataStream ---> Table
        Table inputTable1 = tableEnv.fromDataStream(input1, "name1,account,eventTime1,timestamp1.rowtime");
        Table inputTable2 = tableEnv.fromDataStream(input2, "name2,num,eventTime2,timestamp2.rowtime");

        // timewindow join
        Table resultTable = tableEnv.sqlQuery("select l.name1,l.account,r.num,l.eventTime1,r.eventTime2 " +
                "from " + inputTable1 + " l , " + inputTable2 + " r " +
                " where l.name1 = r.name2 " +
                "and l.timestamp1 - INTERVAL '2' second <= r.timestamp2 " +
                "and r.timestamp2 <= l.timestamp1 + INTERVAL '1' second");
        // Table ---> DataStream
        tableEnv.toAppendStream(resultTable, Row.class)
                .print();
        env.execute("TimeWindowedJoinExample");
    }
}
