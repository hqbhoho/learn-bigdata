package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.EventTimeAndWatermarkExample;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * 窗口聚合操作(滑动窗口)
 * 窗口类型
 * 1. Tumble (Tumbling Windows)
 * 2. Slide (Sliding Windows)
 * 3. Session (Session Windows)
 * <p>
 * Test:
 * nc -l 19999
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109593000
 * hqbhoho,200,1557109595000
 * result
 * Thread: 56,watermark generate, watermark: 1557109591000
 * Thread: 56,watermark generate, watermark: 1557109593000
 * hqbhoho,800,2019-05-06 02:26:30.0,2019-05-06 02:26:33.0
 * Thread: 56,watermark generate, watermark: 1557109593000
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/29
 */
public class TumbleWindowsGroupExample {
    public static void main(String[] args) throws Exception {

        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port1", 19999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // 提取时间戳，生成watermark
        DataStream<Tuple3<String, Integer, Long>> input = env.socketTextStream(host, port)
                .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String line) throws Exception {
                        String[] list = line.split(",");
                        return new Tuple3<String, Integer, Long>(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
                    }
                }).assignTimestampsAndWatermarks(new EventTimeAndWatermarkExample.MyTimeExtractor());

        Table inputTable = tableEnv.fromDataStream(input, "name,account,timestamp.rowtime");
        // 创建窗口
        Table resultTable = inputTable.window(
                Tumble.over("3.seconds")
                        .on("timestamp")
                        .as("window")
        ).groupBy("window,name")
                .select("name,account.sum as sum,window.start,window.end");

        tableEnv.toAppendStream(resultTable, TypeInformation.of(new TypeHint<Row>() {
        }))
                .print();

        env.execute("TumbleWindowsGroupExample");

    }
}
