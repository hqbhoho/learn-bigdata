package com.hqbhoho.bigdata.learnFlink.cep;

import com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream.DetectingPatternsInTablesExample;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
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
 * 下一次模式匹配的起始行设置
 * <p>
 * NO_SKIP	  default          直接从下一行开始匹配，每一行都会作为匹配的起始行
 * SKIP_TO_NEXT                同上
 * SKIP_PAST_LAST_EVENT        没有重复的行，下一次匹配的起始行是当前模式匹配到的最后一行的下一行
 * SKIP_TO_FIRST[b]            下一次匹配的起始行是B模式匹配的第一行
 * SKIP_TO_LAST[b]             下一次匹配的起始行是B模式匹配的最后一行
 * Test:
 * nc -l 19999
 * mixiu,111,2,1557109591000
 * hqbhoho,112,1,1557109591000
 * hqbhoho,115,1,1557109591000
 * hqbhoho,110,1,1557109591000
 * xiaomixi,240,1,1557109591000
 * xiaomixi,113,1,1557109591000
 * hqbhoho,119,4,1557109596000
 * hqbhoho,118,1,1557109591000
 * xiaomixi,242,1,1557109591000
 * xiaomixi,116,1,1557109591000
 * hqbhoho,117,4,1557109596000
 * results:
 * <p>
 * NO_SKIP   AfterMatchSkipStrategy.noSkip()    一行行的匹配    不会跳行    每一行都匹配
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(hqbhoho,115,1,1557109591000), (hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,115,1,1557109591000)], B=[(hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(hqbhoho,115,1,1557109591000), (hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000), (hqbhoho,119,4,1557109596000), (hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,115,1,1557109591000)], B=[(hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000), (hqbhoho,119,4,1557109596000), (hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,119,4,1557109596000), (hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,119,4,1557109596000)], B=[(hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,118,1,1557109591000)], B=[(xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * <p>
 * SKIP_TO_NEXT  AfterMatchSkipStrategy.skipToNext()  和 NO_SKIP 没什么区别
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(hqbhoho,115,1,1557109591000), (hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,115,1,1557109591000)], B=[(hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(hqbhoho,115,1,1557109591000), (hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000), (hqbhoho,119,4,1557109596000), (hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,115,1,1557109591000)], B=[(hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000), (hqbhoho,119,4,1557109596000), (hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,119,4,1557109596000), (hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,119,4,1557109596000)], B=[(hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,118,1,1557109591000)], B=[(xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * <p>
 * SKIP_PAST_LAST_EVENT    AfterMatchSkipStrategy.skipPastLastEvent()   没有重复的行，下一次匹配的起始行是当前模式匹配到的最后一行的下一行
 * 例子中的  第一匹配的最后一行是hqbhoho,119,4,1557109596000 , 那么下一次匹配的起始行就是：hqbhoho,118,1,1557109591000
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(hqbhoho,115,1,1557109591000), (hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,118,1,1557109591000)], B=[(xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * <p>
 * SKIP_TO_FIRST[b],AfterMatchSkipStrategy.skipToFirst("B")   下一次匹配的起始行是B模式匹配的第一行
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(hqbhoho,115,1,1557109591000), (hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,115,1,1557109591000)], B=[(hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,119,4,1557109596000)], B=[(hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * {A=[(hqbhoho,118,1,1557109591000)], B=[(xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 * <p>
 * SKIP_TO_LAST[b], AfterMatchSkipStrategy.skipToLast("B")  下一次匹配的起始行是B模式匹配的最后一行
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(hqbhoho,115,1,1557109591000), (hqbhoho,110,1,1557109591000), (xiaomixi,113,1,1557109591000)], C=[(hqbhoho,119,4,1557109596000)]}
 * {A=[(hqbhoho,119,4,1557109596000)], B=[(hqbhoho,118,1,1557109591000), (xiaomixi,116,1,1557109591000)], C=[(hqbhoho,117,4,1557109596000)]}
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/15
 */
public class AfterMatchSkipStrategyExample {
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
        // 定义模式匹配规则
        Pattern pattern = Pattern.<Tuple4<String, BigDecimal, BigDecimal, Long>>begin("A", AfterMatchSkipStrategy.skipToLast("B"))
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
        PatternStream<Tuple4<String, BigDecimal, BigDecimal, Long>> patternStream = CEP.pattern(input, pattern);

        SingleOutputStreamOperator<Tuple4<String, BigDecimal, BigDecimal, Long>> result = patternStream.process(new PatternProcessFunction<Tuple4<String, BigDecimal, BigDecimal, Long>, Tuple4<String, BigDecimal, BigDecimal, Long>>() {
            @Override
            public void processMatch(Map<String, List<Tuple4<String, BigDecimal, BigDecimal, Long>>> map, Context context, Collector<Tuple4<String, BigDecimal, BigDecimal, Long>> collector) throws Exception {
                Optional.ofNullable(map).ifPresent(System.out::println);
                collector.collect(map.get("C").get(0));
            }
        });
        result.print();
        env.execute("AfterMatchSkipStrategyExample");
    }
}
