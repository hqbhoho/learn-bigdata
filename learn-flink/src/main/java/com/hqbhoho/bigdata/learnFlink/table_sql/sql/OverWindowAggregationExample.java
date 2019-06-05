package com.hqbhoho.bigdata.learnFlink.table_sql.sql;

import com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.EventTimeAndWatermarkExample;
import org.apache.flink.api.common.functions.MapFunction;
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
 * Currently, only windows with PRECEDING (UNBOUNDED and bounded) to CURRENT ROW range are supported.
 * Exception in thread "main" org.apache.flink.table.api.TableException: Unsupported use of OVER windows. The window can only be ordered in ASCENDING mode.
 * <p>
 * {@link com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI.OverWindowsGroupExample}
 * <p>
 * Test:
 * over window定义    以name分组   以timestamp排序  统计[row_num-2,row_num]窗口的数据
 * 触发overwindow  计算的前提是  watermark >= timestamp
 * nc -l 19999
 * hqbhoho,100,1557109591000       1
 * hqbhoho,200,1557109592000       2
 * hqbhoho,101,1557109591000       3
 * hqbhoho,111,1557109591001       4
 * hqbhoho,122,1557109591002       5
 * hqbhoho,300,1557109593000       6
 * hqbhoho,400,1557109594000       7
 * hqbhoho,500,1557109595000       8
 * hqbhoho,600,1557109596000       9
 * <p>
 * result:
 * Thread: 56,watermark generate, watermark: 1557109590000
 * Thread: 56,watermark generate, watermark: 1557109591000
 * hqbhoho,1,100     ---> (1)
 * hqbhoho,2,201     ---> (1,3)
 * Thread: 56,watermark generate, watermark: 1557109591000
 * Thread: 56,watermark generate, watermark: 1557109592000
 * hqbhoho,3,312     --->(1,3,4)
 * hqbhoho,3,334     --->(3,4,5)
 * hqbhoho,3,433     --->(2,4,5)
 * Thread: 56,watermark generate, watermark: 1557109592000
 * Thread: 56,watermark generate, watermark: 1557109593000
 * hqbhoho,3,622     --->(2,5,6)
 * Thread: 56,watermark generate, watermark: 1557109593000
 * Thread: 56,watermark generate, watermark: 1557109594000
 * hqbhoho,3,900     --->(2,6,7)
 * Thread: 56,watermark generate, watermark: 1557109594000
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/05
 */
public class OverWindowAggregationExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port1", 19999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);

        // 提取时间戳，生成watermark
        DataStream<Tuple3<String, Integer, Long>> input = env.socketTextStream(host, port)
                .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String line) throws Exception {
                        String[] list = line.split(",");
                        return new Tuple3<String, Integer, Long>(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
                    }
                }).assignTimestampsAndWatermarks(new EventTimeAndWatermarkExample.MyTimeExtractor());

        // DataStream ---> Table
        Table inputTable = tableEnv.fromDataStream(input, "name,account,eventtime.rowtime");

        Table resultTable = tableEnv.sqlQuery(
                "select name,count(account) over w,sum(account) over w " +
                        "from " + inputTable + " Window w as " +
                        "(" +
                        "partition by name " +
                        "order by eventtime " +
                        "rows between 2 PRECEDING AND CURRENT ROW" +
                        ")"
        );

        // Table ---> DataStream
        tableEnv.toAppendStream(resultTable, Row.class)
                .print();

        env.execute("OverWindowAggregationExample");
    }
}
