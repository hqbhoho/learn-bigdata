package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.EventTimeAndWatermarkExample;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.Over;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * 类似于sql中的开窗函数
 * <p>
 * Test
 * over window定义    以name分组   以timestamp排序   统计[timestamp-2000,timestamp]窗口的数据
 * nc -l 19999
 * hqbhoho,100,1557109591000       watermark generate, watermark: 1557109589000    do nothing
 * hqbhoho,200,1557109592000       watermark generate, watermark: 1557109590000    do nothing
 * hqbhoho,101,1557109591000       watermark generate, watermark: 1557109590000    do nothing
 * hqbhoho,111,1557109591001       watermark generate, watermark: 1557109590000    do nothing
 * hqbhoho,122,1557109591002       watermark generate, watermark: 1557109590000    do nothing
 * hqbhoho,300,1557109593000       watermark generate, watermark: 1557109591000    hqbhoho,100,1557109591000   hqbhoho,101,1557109591000 trigger over window compute
 * hqbhoho,400,1557109594000       watermark generate, watermark: 1557109592000    hqbhoho,111,1557109591001   hqbhoho,122,1557109591002 hqbhoho,200,1557109592000 trigger over window compute
 * hqbhoho,500,1557109595000       watermark generate, watermark: 1557109593000    hqbhoho,300,1557109593000   trigger over window compute
 * hqbhoho,600,1557109596000       watermark generate, watermark: 1557109594000    hqbhoho,400,1557109594000   trigger over window compute
 * result
 * Thread: 56,watermark generate, watermark: 1557109590000
 * Thread: 56,watermark generate, watermark: 1557109591000
 * hqbhoho,201,101,100
 * hqbhoho,201,101,100
 * Thread: 56,watermark generate, watermark: 1557109591000
 * Thread: 56,watermark generate, watermark: 1557109592000
 * hqbhoho,312,111,100
 * hqbhoho,434,122,100
 * hqbhoho,634,200,100
 * Thread: 56,watermark generate, watermark: 1557109592000
 * Thread: 56,watermark generate, watermark: 1557109593000
 * hqbhoho,934,300,100
 * Thread: 56,watermark generate, watermark: 1557109593000
 * Thread: 56,watermark generate, watermark: 1557109594000
 * hqbhoho,900,400,200
 * Thread: 56,watermark generate, watermark: 1557109594000
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/29
 */
public class OverWindowsGroupExample {
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

        Table inputTable = tableEnv.fromDataStream(input, "name,account,timestamp.rowtime");

        Table resultTable = inputTable.window(
                Over.partitionBy("name")
                        .orderBy("timestamp")
                        .preceding("2.second")
                        .following("CURRENT_RANGE")
                        .as("window")
        )
                .select("name,account.sum over window,account.max over window,account.min over window");
        tableEnv.toAppendStream(resultTable, Row.class)
                .print();
        env.execute("OverWindowsGroupExample");
    }
}
