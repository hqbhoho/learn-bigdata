package com.hqbhoho.bigdata.learnFlink.cep;

import com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream.DetectingPatternsInTablesExample;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * describe:
 * <p>
 * Whenever a pattern has a window length attached via the within keyword, it is possible that partial event sequences are discarded because they exceed the window length.
 * Test:
 * nc -l 1999
 * hqbhoho,240,2,1557109592000
 * hqbhoho,240,4,1557109593000
 * hqbhoho,112,2,1557109594000
 * hqbhoho,114,4,1557109595000 (after 5 min)
 * results:
 * patternStream{A=[(hqbhoho,240,4,1557109593000)]}
 * patternStream{A=[(hqbhoho,240,4,1557109593000), (hqbhoho,112,2,1557109594000)]}
 * patternStream{A=[(hqbhoho,112,2,1557109594000)]}
 * (hqbhoho,114,4,1557109595000)
 * OutputTag{A=[(hqbhoho,240,4,1557109593000), (hqbhoho,112,2,1557109594000)]}
 * OutputTag{A=[(hqbhoho,112,2,1557109594000)]}
 * patternStream{A=[(hqbhoho,114,4,1557109595000)]}
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/15
 */
public class TimedOutPartialPatternsExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "192.168.5.131");
        int port1 = tool.getInt("port1", 19999);
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 获取数据
        DataStream<Tuple4<String, BigDecimal, BigDecimal, Long>> input = env.socketTextStream(host, port1)
                .map(new DetectingPatternsInTablesExample.TokenFunction4());

        // A : value.f0.contains("hqb") AND (value.f1.intValue() - 200 < 0 OR value.f2.intValue() > 3)
        Pattern A = Pattern.<Tuple4<String, BigDecimal, BigDecimal, Long>>begin("A").where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
            @Override
            public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                return value.f0.contains("hqb");
            }
        }).where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
            @Override
            public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                return value.f1.intValue() - 200 < 0;
            }
        }).or(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
            @Override
            public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                return value.f2.intValue() > 3;
            }
        })
                .oneOrMore()
                //类似状态会有过期时间
                .within(Time.minutes(3));
        //.greedy();

        PatternStream<Tuple4<String, BigDecimal, BigDecimal, Long>> patternStream = CEP.pattern(input, A);
        OutputTag outputTag = new OutputTag<Tuple4<String, BigDecimal, BigDecimal, Long>>("side-output") {
        };

        SingleOutputStreamOperator result = patternStream.flatSelect(outputTag,
                // 处理过时的消息
                new PatternFlatTimeoutFunction<Tuple4<String, BigDecimal, BigDecimal, Long>, Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public void timeout(Map<String, List<Tuple4<String, BigDecimal, BigDecimal, Long>>> map, long l, Collector<Tuple4<String, BigDecimal, BigDecimal, Long>> collector) throws Exception {
                        Optional.ofNullable("OutputTag" + map).ifPresent(System.out::println);
                        collector.collect(map.get("A").get(0));
                    }
                    // 处理正常的消息
                }, new PatternFlatSelectFunction<Tuple4<String, BigDecimal, BigDecimal, Long>, Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public void flatSelect(Map<String, List<Tuple4<String, BigDecimal, BigDecimal, Long>>> map, Collector<Tuple4<String, BigDecimal, BigDecimal, Long>> collector) throws Exception {
                        Optional.ofNullable("patternStream" + map).ifPresent(System.out::println);
                        collector.collect(map.get("A").get(0));
                    }
                });
        result.print();
        result.getSideOutput(outputTag).print();
        env.execute("TimedOutPartialPatternsExample");
    }
}
