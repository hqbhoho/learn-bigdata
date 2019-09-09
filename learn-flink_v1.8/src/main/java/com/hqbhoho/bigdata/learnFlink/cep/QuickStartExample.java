package com.hqbhoho.bigdata.learnFlink.cep;

import com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream.DetectingPatternsInTablesExample;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * describe:
 * <p>
 * Test:
 * Pattern A: 输入的第一个字段中包含"hqb"
 * Pattren B: 输入的第二个字段累加值不超过500
 * Pattern C: 输入的第三个字段不超过3
 * A B+（relaxed contiguity） C
 * <p>
 * nc -l 19999
 * mixiu,110,2,1557109591000
 * hqbhoho,112,1,1557109591000
 * xiaomixi,113,1,1557109591000
 * hqbhoho,111,2,1557109592000
 * xiaomixi,113,1,1557109591000
 * hqbhoho,110,1,1557109593000
 * hqbhoho,109,1,1557109594000
 * hqbhoho,108,2,1557109595000
 * hqbhoho,120,4,1557109596000
 * results:
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,111,2,1557109592000), (xiaomixi,113,1,1557109591000), (hqbhoho,110,1,1557109593000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,111,2,1557109592000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,110,1,1557109593000), (hqbhoho,109,1,1557109594000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109593000)], B=[(hqbhoho,109,1,1557109594000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,109,1,1557109594000)], B=[(hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/14
 */
public class QuickStartExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 时间属性
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);

        DataStream<Tuple4<String, BigDecimal, BigDecimal, Long>> input = env.socketTextStream(host, port1)
                .map(new DetectingPatternsInTablesExample.TokenFunction4());
//                .assignTimestampsAndWatermarks(new DetectingPatternsInTablesExample.MyTimeExtractor4(2000));
        // 定义规则 A
        Pattern A = Pattern.<Tuple4<String, BigDecimal, BigDecimal, Long>>begin("A")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f0.contains("hqb");
                    }
                });
        // 定义规则 B
        Pattern B = Pattern.<Tuple4<String, BigDecimal, BigDecimal, Long>>begin("B")
                .where(new IterativeCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value, Context<Tuple4<String, BigDecimal, BigDecimal, Long>> context) throws Exception {
                        BigDecimal sum = value.f1;
                        for (Tuple4<String, BigDecimal, BigDecimal, Long> event : context.getEventsForPattern("B")) {
                            sum = sum.add(value.f1);
                        }
                        return sum.intValue() - 500 <= 0;
                    }
                });
        // 定义规则 C
        Pattern C = Pattern.<Tuple4<String, BigDecimal, BigDecimal, Long>>begin("C")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f2.intValue() > 3;
                    }
                });
        // 组合规则
        GroupPattern pattern = Pattern.begin(A).followedBy(B).oneOrMore().next(C);

        PatternStream<Tuple4<String, BigDecimal, BigDecimal, Long>> patternStream = CEP.pattern(input, pattern);
        // 结果输出
        SingleOutputStreamOperator<Tuple4<String, BigDecimal, BigDecimal, Long>> result = patternStream.process(new PatternProcessFunction<Tuple4<String, BigDecimal, BigDecimal, Long>, Tuple4<String, BigDecimal, BigDecimal, Long>>() {
            @Override
            public void processMatch(Map<String, List<Tuple4<String, BigDecimal, BigDecimal, Long>>> map, Context context, Collector<Tuple4<String, BigDecimal, BigDecimal, Long>> collector) throws Exception {
                Optional.ofNullable(map).ifPresent(System.out::println);
                collector.collect(map.get("C").get(0));
            }
        });

        result.print();
        env.execute("QuickStartExample");
    }
}
