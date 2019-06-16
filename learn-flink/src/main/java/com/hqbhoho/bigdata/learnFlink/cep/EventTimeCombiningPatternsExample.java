package com.hqbhoho.bigdata.learnFlink.cep;

import com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream.DetectingPatternsInTablesExample;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * describe:
 * <p>
 * 基于Event time 的模式匹配
 * Test:
 * nc -l 19999
 * mixiu,110,2,1557109590000
 * hqbhoho,112,1,1557109591000
 * xiaomixi,113,1,1557109592000
 * hqbhoho,111,2,1557109592000
 * hqbhoho,110,2,1557109592000
 * xiaomixi,240,1,1557109594000
 * hqbhoho,119,1,1557109591000
 * hqbhoho,114,3,1557109595000
 * hqbhoho,114,2,1557109595000
 * hqbhoho,120,4,1557109597000
 * xiaoxiao,114,2,1557109596000
 * hqbhoho,116,2,1557109598000
 * hqbhoho,117,2,1557109599000
 * results:
 * Thread: 57,watermark generate, watermark: 1557109592000
 * late data: (hqbhoho,119,1,1557109591000)
 * Thread: 57,watermark generate, watermark: 1557109592000
 * Thread: 57,watermark generate, watermark: 1557109595000
 * Thread: 57,watermark generate, watermark: 1557109596000
 * Thread: 57,watermark generate, watermark: 1557109597000
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(hqbhoho,110,2,1557109592000), (hqbhoho,111,2,1557109592000), (xiaomixi,113,1,1557109592000), (hqbhoho,114,2,1557109595000), (hqbhoho,114,3,1557109595000), (xiaoxiao,114,2,1557109596000)], C=[(hqbhoho,120,4,1557109597000)]}
 * (hqbhoho,112,1,1557109591000)
 * {A=[(hqbhoho,110,2,1557109592000)], B=[(hqbhoho,111,2,1557109592000), (xiaomixi,113,1,1557109592000), (hqbhoho,114,2,1557109595000), (hqbhoho,114,3,1557109595000), (xiaoxiao,114,2,1557109596000)], C=[(hqbhoho,120,4,1557109597000)]}
 * (hqbhoho,110,2,1557109592000)
 * {A=[(hqbhoho,111,2,1557109592000)], B=[(xiaomixi,113,1,1557109592000), (hqbhoho,114,2,1557109595000), (hqbhoho,114,3,1557109595000), (xiaoxiao,114,2,1557109596000)], C=[(hqbhoho,120,4,1557109597000)]}
 * (hqbhoho,111,2,1557109592000)
 * {A=[(hqbhoho,114,2,1557109595000)], B=[(hqbhoho,114,3,1557109595000), (xiaoxiao,114,2,1557109596000)], C=[(hqbhoho,120,4,1557109597000)]}
 * (hqbhoho,114,2,1557109595000)
 * {A=[(hqbhoho,114,3,1557109595000)], B=[(xiaoxiao,114,2,1557109596000)], C=[(hqbhoho,120,4,1557109597000)]}
 * (hqbhoho,114,3,1557109595000)
 * Thread: 57,watermark generate, watermark: 1557109597000
 * 小结：
 * 此时watermark >= 匹配到的最后一行的timestamp   trigger compute
 * 并且相同时间戳的event会根据自定义的EventComparator排序{@link MyEventComparator}
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/15
 */
public class EventTimeCombiningPatternsExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "192.168.5.131");
        int port1 = tool.getInt("port1", 19999);
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 时间属性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        // 获取数据
        DataStream<Tuple4<String, BigDecimal, BigDecimal, Long>> input = env.socketTextStream(host, port1)
                .map(new DetectingPatternsInTablesExample.TokenFunction4())
                .assignTimestampsAndWatermarks(new DetectingPatternsInTablesExample.MyTimeExtractor4(2000));

        // 定义模式匹配规则  A B+ C
        Pattern pattern = Pattern.<Tuple4<String, BigDecimal, BigDecimal, Long>>begin("A")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f0.contains("hqb");
                    }
                })
                .followedBy("B")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f1.intValue() - 200 < 0;
                    }
                })
                .oneOrMore()
                .next("C")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f2.intValue() > 3;
                    }
                });
        // 模式规则匹配
        PatternStream<Tuple4<String, BigDecimal, BigDecimal, Long>> patternStream = CEP.pattern(input, pattern, new MyEventComparator());
        // 迟到的消息处理
        OutputTag lateDataOutputTag = new OutputTag<Tuple4<String, BigDecimal, BigDecimal, Long>>("late-data") {
        };
        SingleOutputStreamOperator results = patternStream.sideOutputLateData(lateDataOutputTag)
                .select(new PatternSelectFunction<Tuple4<String, BigDecimal, BigDecimal, Long>, Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public Tuple4<String, BigDecimal, BigDecimal, Long> select(Map<String, List<Tuple4<String, BigDecimal, BigDecimal, Long>>> map) throws Exception {
                        Optional.ofNullable(map).ifPresent(System.out::println);
                        return map.get("A").get(0);
                    }
                });

        // 获取迟到的数据   watermark  以后到达的数据
        results.print();
        results.getSideOutput(lateDataOutputTag).map(value -> "late data: " + value).print();
        env.execute("EventTimeCombiningPatternsExample");
    }

    /**
     * 用于event time  相同时按照  f0 f1 f2的字段顺序进行排序
     */
    public static class MyEventComparator implements EventComparator<Tuple4<String, BigDecimal, BigDecimal, Long>> {

        @Override
        public int compare(Tuple4<String, BigDecimal, BigDecimal, Long> value1, Tuple4<String, BigDecimal, BigDecimal, Long> value2) {
            int result = 0;
            if (!value1.f0.equalsIgnoreCase(value2.f0)) {
                result = value1.f0.compareTo(value2.f0);
            } else if (!value1.f1.equals(value2.f1)) {
                result = value1.f1.compareTo(value2.f1);
            } else if (!value1.f2.equals(value2.f2)) {
                result = value1.f2.compareTo(value2.f2);
            }
            return result;
        }

    }
}
