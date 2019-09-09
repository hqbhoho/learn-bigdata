package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

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
 * 有点类似于 interval join {@link com.hqbhoho.bigdata.learnFlink.streaming.operators.IntervalJoinExample}
 * <p>
 * A time-windowed join requires
 * at least one equi-join predicate
 * and a join condition that bounds the time on both sides.
 * Test: 基于event time
 * nc -l 19999
 * hqbhoho01,10,1557109591000     1         do nothing
 * hqbhoho01,11,1557109591001     6         output:  6&&2 , 6&&3 , 6&&4
 * hqbhoho01,20,1557109592000     7         output:  7&&2 , 7&&3 , 7&&4 , 7&&5
 * hqbhoho01,12,1557109591002     8         output:  8&&2 , 8&&3 , 8&&4
 * hqbhoho01,30,1557109593000     12        output:  12&&2 , 12&&3 , 12&&4 , 12&&5 , 12&&9 , 12&&10 , 12&&11
 *
 * nc -l 29999
 * hqbhoho01,100,1557109591000    2        output:   1&&2
 * hqbhoho01,101,1557109591001    3        output:   1&&3
 * hqbhoho01,200,1557109592000    4        output:   1&&4
 * hqbhoho01,300,1557109593000    5        do nothing
 * hqbhoho01,102,1557109591002    9        output：  1&&9 , 6&&9 , 7&&9 , 8&&9
 * hqbhoho01,201,1557109592001    10       output:   6&&10 , 7&&10 , 8&&10
 * hqbhoho01,400,1557109594000    11       do nothing
 *
 * result:
 * hqbhoho01,10,100,1557109591000,1557109591000
 * hqbhoho01,10,101,1557109591000,1557109591001
 * hqbhoho01,10,200,1557109591000,1557109592000
 * hqbhoho01,11,100,1557109591001,1557109591000
 * hqbhoho01,11,101,1557109591001,1557109591001
 * hqbhoho01,11,200,1557109591001,1557109592000
 * hqbhoho01,20,100,1557109592000,1557109591000
 * hqbhoho01,20,300,1557109592000,1557109593000
 * hqbhoho01,20,101,1557109592000,1557109591001
 * hqbhoho01,20,200,1557109592000,1557109592000
 * hqbhoho01,12,100,1557109591002,1557109591000
 * hqbhoho01,12,101,1557109591002,1557109591001
 * hqbhoho01,12,200,1557109591002,1557109592000
 * hqbhoho01,10,102,1557109591000,1557109591002
 * hqbhoho01,11,102,1557109591001,1557109591002
 * hqbhoho01,12,102,1557109591002,1557109591002
 * hqbhoho01,20,102,1557109592000,1557109591002
 * hqbhoho01,11,201,1557109591001,1557109592001
 * hqbhoho01,12,201,1557109591002,1557109592001
 * hqbhoho01,20,201,1557109592000,1557109592001
 * hqbhoho01,30,100,1557109593000,1557109591000
 * hqbhoho01,30,300,1557109593000,1557109593000
 * hqbhoho01,30,101,1557109593000,1557109591001
 * hqbhoho01,30,102,1557109593000,1557109591002
 * hqbhoho01,30,200,1557109593000,1557109592000
 * hqbhoho01,30,400,1557109593000,1557109594000
 * hqbhoho01,30,201,1557109593000,1557109592001
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/30
 */
public class TimeWindowJoinExample {
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
        Table resultTable = inputTable1.join(inputTable2)
                .where("name1 = name2 && timestamp1 - 2.seconds <= timestamp2 && timestamp2 <= timestamp1 + 1.seconds")
                .select("name1,account,num,eventTime1,eventTime2");

        tableEnv.toAppendStream(resultTable, Row.class)
                .print();

        env.execute("TimeWindowJoinExample");
    }
}
