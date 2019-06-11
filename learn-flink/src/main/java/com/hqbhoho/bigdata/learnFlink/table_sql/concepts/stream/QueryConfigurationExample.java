package com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream;

import com.hqbhoho.bigdata.learnFlink.streaming.operators.WindowJoinExample;
import com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.EventTimeAndWatermarkExample;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 *
 *  有问题 ，测试未成功！！！
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/06
 */
public class QueryConfigurationExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 时间属性
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000000);

        // 设置key的过期时间
        StreamQueryConfig streamQueryConfig = tableEnv.queryConfig();
        streamQueryConfig.withIdleStateRetentionTime(Time.minutes(1L), Time.minutes(6L));

        DataStream<Tuple3<String, Integer, Long>> input = env.socketTextStream(host, port1)
                .map(new WindowJoinExample.TokenFunction())
                .assignTimestampsAndWatermarks(new EventTimeAndWatermarkExample.MyTimeExtractor());

        // DataStream ---> Table
        Table inputTable = tableEnv.fromDataStream(input, "name,account,eventTime.rowtime");
        // query
        Table resultTable = tableEnv.sqlQuery("select name,sum(account) from " + inputTable + " group by name");
        // Table ---> DataStream
        tableEnv.toRetractStream(resultTable, Row.class,streamQueryConfig)
                .print();

        env.execute("QueryConfigurationExample");
    }
}
