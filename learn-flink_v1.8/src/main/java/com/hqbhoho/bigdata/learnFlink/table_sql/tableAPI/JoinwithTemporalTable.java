package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * describe:
 * <p>
 * processtime: Only the latest versions (with respect to the defined primary key) of the build side records are kept in the state.
 * eventtime: not only keep the latest version (with respect to the defined primary key) of the build side records in the state but stores all versions (identified by time) since the last watermark.
 * <p>
 * 此处以  event time 为例   注意watermark
 * <p>
 * Test:
 * nc -l 19999
 * hqbhoho01,CN RMB,100.00,1557109591010  1   do nothing
 * hqbhoho01,CN RMB,100.00,1557109594009  3   do nothing
 * hqbhoho01,CN RMB,100.00,1557109594010  5   do nothing
 * nc -l 29999
 * CN RMB,7.0,1557109591000      2     do nothing
 * CN RMB,8.0,1557109592000      4     do nothing
 * CN RMB,9.0,1557109592010      6     output
 * <p>
 * result:
 * Thread: 59,watermark generate, watermark: 1557109591010
 * Thread: 58,watermark generate, watermark: 1557109591000
 * Thread: 59,watermark generate, watermark: 1557109591010
 * Thread: 58,watermark generate, watermark: 1557109591010
 * hqbhoho01,CN RMB,700.000,2019-05-06 02:26:31.01,2019-05-06 02:26:31.0
 * Thread: 59,watermark generate, watermark: 1557109591010
 * <p>
 * 总结： 消息1触发计算的前提是   两个流的watermark都要大于消息1的timestamp
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/31
 */
public class JoinwithTemporalTable {
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
        env.getConfig().setAutoWatermarkInterval(1000);

        // 模拟数据源
        DataStream<Tuple4<String, String, BigDecimal, Long>> accountsInput = env.socketTextStream(host, port1)
                .map(new TokenFunction4())
                .assignTimestampsAndWatermarks(new MyTimeExtractor4(3000));

        DataStream<Tuple3<String, BigDecimal, Long>> ratesInput = env.socketTextStream(host, port2)
                .map(new TokenFunction3())
                .assignTimestampsAndWatermarks(new MyTimeExtractor3(1000));

        // DataStream ---> Table
        tableEnv.registerDataStream("accounts", accountsInput, "name,currency,account,timestamp1.rowtime");
        Table ratesTable = tableEnv.fromDataStream(ratesInput, "bz,rate,timestamp2.rowtime");
        // 创建并注册 Temporal Table Function
        TemporalTableFunction ratesFunction = ratesTable.createTemporalTableFunction("timestamp2", "bz");
        tableEnv.registerFunction("rateConvet", ratesFunction);

        // Table API
        Table resultTbale1 = tableEnv.scan("accounts")
                .joinLateral("rateConvet(timestamp1)", "currency = bz")
                .select("name,currency,account * rate,timestamp1,timestamp2");

        // SQL API
        Table resultTable2 = tableEnv.sqlQuery("select name,currency,account * rate,timestamp1,timestamp2 " +
                "from accounts , Lateral table (rateConvet(timestamp1)) " +
                "where currency = bz");

        // Table   ---> DataStream
        /*tableEnv.toAppendStream(resultTbale1, Row.class)
                .print();*/
        tableEnv.toAppendStream(resultTable2, Row.class)
                .print();
        env.execute("JoinwithTemporalTable");
    }

    /**
     * 分词
     */
    public static class TokenFunction3 implements MapFunction<String, Tuple3<String, BigDecimal, Long>> {

        @Override
        public Tuple3<String, BigDecimal, Long> map(String value) throws Exception {
            String[] split = value.split(",");
            return Tuple3.of(split[0], new BigDecimal(split[1]), Long.valueOf(split[2]));
        }
    }

    public static class TokenFunction4 implements MapFunction<String, Tuple4<String, String, BigDecimal, Long>> {

        @Override
        public Tuple4<String, String, BigDecimal, Long> map(String value) throws Exception {
            String[] split = value.split(",");
            return Tuple4.of(split[0], split[1], new BigDecimal(split[2]), Long.valueOf(split[3]));
        }
    }

    /**
     * time extract and watermark generate
     */
    public static class MyTimeExtractor3 implements AssignerWithPeriodicWatermarks<Tuple3<String, BigDecimal, Long>> {

        private long maxOutOfOrderness; // 2 seconds

        private long currentMaxTimestamp = 0;

        public MyTimeExtractor3() {
            this.maxOutOfOrderness = 2000;
        }

        public MyTimeExtractor3(long maxOutOfOrderness) {
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        @Override
        public Watermark getCurrentWatermark() {
            Optional.ofNullable("Thread: " + Thread.currentThread().getId() + ",watermark generate, watermark: " + (currentMaxTimestamp - maxOutOfOrderness)).ifPresent(System.out::println);
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3 tuple3, long l) {
            long timeStamp = (long) tuple3.f2;
            currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
            return timeStamp;
        }
    }

    public static class MyTimeExtractor4 implements AssignerWithPeriodicWatermarks<Tuple4<String, String, BigDecimal, Long>> {

        private long maxOutOfOrderness; // 2 seconds

        private long currentMaxTimestamp = 0;

        public MyTimeExtractor4() {
            this.maxOutOfOrderness = 2000;
        }

        public MyTimeExtractor4(long maxOutOfOrderness) {
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        @Override
        public Watermark getCurrentWatermark() {
            Optional.ofNullable("Thread: " + Thread.currentThread().getId() + ",watermark generate, watermark: " + (currentMaxTimestamp - maxOutOfOrderness)).ifPresent(System.out::println);
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple4 tuple4, long l) {
            long timeStamp = (long) tuple4.f3;
            currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
            return timeStamp;
        }
    }
}
