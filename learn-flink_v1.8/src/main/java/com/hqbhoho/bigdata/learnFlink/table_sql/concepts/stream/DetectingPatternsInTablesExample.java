package com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * describe:
 * sql   行模式匹配
 * <p>
 * PARTITION BY       : 类似keyby操作，可以并行处理，如果没有此参数  并行度为1
 * ORDER BY           : 排序 第一个排序规则必须是按照时间的升序 ORDER BY rowtime ASC, price DESC is valid but ORDER BY price, rowtime or ORDER BY rowtime DESC, price ASC is not
 * MEASURES           : 类似sql 中的select
 * ONE ROW PER MATCH  : 模式匹配的输出结果，目前只支持输出一条结果(聚合值)，不支持 ALL ROWS PER MATCH
 * AFTER MATCH SKIP   : 设置下一次模式匹配的起始行
 * ----SKIP PAST LAST ROW : 这一次模式匹配，匹配到的最后一行的下一行开始新的匹配   event 只会被使用一次
 * ----SKIP TO NEXT ROW : 这一次模式匹配，匹配到的第一行的下一行开始新的匹配
 * ----SKIP TO LAST variable : 这一次模式匹配，variable规则匹配到的最后一行开始新的匹配
 * ----SKIP TO FIRST variable : 这一次模式匹配，variable规则匹配到的第一行开始新的匹配   (死循环,禁用）
 * PATTERN            :   定义模式匹配的规则序列   eg：A B+ C
 * ----PATTERN (A B* C) WITHIN INTERVAL '1' HOUR   可以限制模式匹配的事件的时间跨度，从而保证Flink状态不会太大。 standard sql 没有within 语法
 * ----Please note that the MATCH_RECOGNIZE clause does not use a configured state retention time. One may want to use the WITHIN clause for this purpose.
 * DEFINE             ：  各个规则的实际定义
 * <p>
 * Pattern Navigation
 * 1. Pattern Variable Referencing
 * PATTERN (A B+)
 * DEFINE
 * A AS A.price > 10,
 * B AS B.price > A.price AND SUM(price) < 100 AND SUM(B.price) < 80
 * 其中 B.price > A.price 中 A.price是A规则匹配记录集的最后一条记录的price
 * 其中 SUM(price) < 100 中 price是不区分事件，就是所有记录集的price的总和。
 * 2. Logical Offsets
 * LAST(variable.field, n)
 * variable规则匹配到的倒数第n记录的field值，计数从第一个已经匹配到variable规则的记录开始
 * FIRST(variable.field, n)
 * variable规则匹配到的正数第n记录的field值，计数从第一个已经匹配到variable规则的记录开始
 * <p>
 * Test:
 * 以官网股价为例，演示
 *    1，2，3，4，5，6 符合 A+ B  但由于 WITHIN INTERVAL '5' second，所以当watermark >= 1557109596000时 2，3，4，5，6 会被当做第一个
 * nc -l 19999
 * hqbhoho,112,1,1557109591000    1      watermark: 1557109589000；record: timestamp <=1557109589000 no match
 * hqbhoho,111,2,1557109592000    2      watermark: 1557109590000；record: timestamp <=1557109590000 no match
 * hqbhoho,110,1,1557109593000    3      watermark: 1557109591000；record: timestamp <=1557109591000 (1) no match
 * hqbhoho,109,3,1557109594000    4      watermark: 1557109592000；record: timestamp <=1557109592000 (1,2) no match
 * hqbhoho,108,2,1557109595000    5      watermark: 1557109593000；record: timestamp <=1557109593000 (1,2,3) no match
 * hqbhoho,120,1,1557109596000    6      watermark: 1557109594000；record: timestamp <=1557109593000 (1,2,3,4) no match
 * hqbhoho,120,1,1557109597000    7      watermark: 1557109595000；record: timestamp <=1557109595000 (1,2,3,4,5) no match
 * hqbhoho,119,2,1557109598000    8      watermark: 1557109596000；record: timestamp <=1557109596000 (1,2,3,4,5,6) WITHIN INTERVAL '5' second 1 will be remove (2,3,4,5,6) match ,trigger compute ,result:hqbhoho,2019-05-06 02:26:32.0,2019-05-06 02:26:35.0,111,108,218.75,next match will begin 7
 * hqbhoho,118,2,1557109599000    9      watermark: 1557109597000；record: timestamp <=1557109597000 (7) no match
 * hqbhoho,130,2,1557109600000    10     watermark: 1557109598000；record: timestamp <=1557109598000 (7,8) no match
 * hqbhoho,124,1,1557109598010    11     watermark: 1557109598000；record: timestamp <=1557109598000 (7,8,) no match
 * hqbhoho,130,2,1557109600010    12     watermark: 1557109598010；record: timestamp <=1557109598000 (7,8,11) ,trigger compute ,result:hqbhoho,2019-05-06 02:26:37.0,2019-05-06 02:26:38.0,120,119,179
 * results:
 * Thread: 56,watermark generate, watermark: 1557109589000
 * Thread: 56,watermark generate, watermark: 1557109590000
 * Thread: 56,watermark generate, watermark: 1557109591000
 * Thread: 56,watermark generate, watermark: 1557109592000
 * Thread: 56,watermark generate, watermark: 1557109593000
 * Thread: 56,watermark generate, watermark: 1557109594000
 * Thread: 56,watermark generate, watermark: 1557109595000
 * Thread: 56,watermark generate, watermark: 1557109596000
 * hqbhoho,2019-05-06 02:26:32.0,2019-05-06 02:26:35.0,111,108,218.75
 * Thread: 56,watermark generate, watermark: 1557109596000
 * Thread: 56,watermark generate, watermark: 1557109597000
 * Thread: 56,watermark generate, watermark: 1557109598000
 * Thread: 56,watermark generate, watermark: 1557109598010
 * hqbhoho,2019-05-06 02:26:37.0,2019-05-06 02:26:38.0,120,119,179
 * Thread: 56,watermark generate, watermark: 1557109598010
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/05
 */
public class DetectingPatternsInTablesExample {
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
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStream<Tuple4<String, BigDecimal, BigDecimal, Long>> input = env.socketTextStream(host, port1)
                .map(new TokenFunction4())
                .assignTimestampsAndWatermarks(new MyTimeExtractor4(2000));
        // DataStream ---> Table
        Table inputTable = tableEnv.fromDataStream(input, "symbol,price,tax,eventTime.rowtime");
        // 行模式匹配
        // A事件：股价下跌   B事件：股价上涨
        Table resultTable = tableEnv.sqlQuery("select * from " + inputTable +
                "  MATCH_RECOGNIZE ( " +
                "      partition by symbol " +
                "      order by eventTime  " +
                "      measures " +
                "           FIRST(A.eventTime) AS start_stamp," +
                "           LAST(A.eventTime) AS lowset_stamp," +
                "           FIRST(A.price) AS start_price," +
                "           LAST(A.price) AS lowest_price," +
                "           AVG(A.price * A.tax) AS lowest_intelval_avgTaxAccount " +
                "      ONE ROW PER MATCH " +
                "      AFTER MATCH SKIP PAST LAST ROW " +
                "      PATTERN (A+ B) WITHIN INTERVAL '5' second" +
                "      DEFINE " +
                "           A AS LAST(A.price,1) IS NULL OR A.price < LAST(A.price,1)," +
                "           B AS B.price > LAST(A.price,1)" +
                "    ) MR");

        // Table  ---> DataStream
        tableEnv.toAppendStream(resultTable, Row.class).print();

        env.execute("DetectingPatternsInTablesExample");
    }

    /**
     * 分词
     */
    public static class TokenFunction4 implements MapFunction<String, Tuple4<String, BigDecimal, BigDecimal, Long>> {

        @Override
        public Tuple4<String, BigDecimal, BigDecimal, Long> map(String value) throws Exception {
            String[] split = value.split(",");
            return Tuple4.of(split[0], new BigDecimal(split[1]), new BigDecimal(split[2]), Long.valueOf(split[3]));
        }
    }

    /**
     * extract timestamp and watermark generate
     */
    public static class MyTimeExtractor4 implements AssignerWithPeriodicWatermarks<Tuple4<String, BigDecimal, BigDecimal, Long>> {

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
