package com.hqbhoho.bigdata.learnFlink.cep;

import com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream.DetectingPatternsInTablesExample;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
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
 * <p>
 * 模式匹配组合
 * A规则： value.f0.contains("hqb")
 * B规则： value.f1.intValue() - 200 < 0
 * C规则： value.f2.intValue() > 3
 * A B+ C
 * Test:
 * nc -l 19999
 * mixiu,110,2,1557109591000             1
 * hqbhoho,112,1,1557109591000           2
 * xiaomixi,113,1,1557109591000          3
 * hqbhoho,111,2,1557109592000           4
 * xiaomixi,240,1,1557109591000          5
 * hqbhoho,110,1,1557109593000           6
 * hqbhoho,108,2,1557109595000           7
 * hqbhoho,120,4,1557109596000           8
 * results:
 * <p>
 * default Relaxed Contiguity
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,111,2,1557109592000), (hqbhoho,110,1,1557109593000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,111,2,1557109592000)], B=[(hqbhoho,110,1,1557109593000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109593000)], B=[(hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * <p>
 * strict contiguity
 * {A=[(hqbhoho,111,2,1557109592000)], B=[(hqbhoho,110,1,1557109593000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109593000)], B=[(hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * <p>
 * non-deterministic relaxed contiguity
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,111,2,1557109592000), (hqbhoho,110,1,1557109593000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,111,2,1557109592000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,110,1,1557109593000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,112,1,1557109591000)], B=[(xiaomixi,113,1,1557109591000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,111,2,1557109592000)], B=[(hqbhoho,110,1,1557109593000), (hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * {A=[(hqbhoho,110,1,1557109593000)], B=[(hqbhoho,108,2,1557109595000)], C=[(hqbhoho,120,4,1557109596000)]}
 * <p>
 * 自己的一点理解：Contiguity  结合上述的案例
 * 1. strict contiguity
 *    不能容忍中断    A B+ C  必须满足这个模式 A B B* B C
 *    eg:  A B1 B2 B3 C -->  A B1 B2 B3 C , A B C ---> A B C , A C B1 B2 B3 C --> A B1 B2 B3 C
 * 2. Relaxed Contiguity
 *    能够容忍中断（及去除AC之间的非B事件）   A B+ C   必须满足这个模式 A B 任意事件*(但是其中B不能随意组合，只能是匹配所有) B C
 *    eg:  A D1 B1 D1 B2 D2 B3 C -->  A B1 B2 B3 C
 *    B3 C 事件必须有   所以此处只有  A B1 B2 B3 C
 *    不容忍跨匹配到的事件进行匹配  比如  A B1 B3 C
 * 3. non-deterministic relaxed contiguity
 *    最大限度的容忍中断（及去除AC之间的非B事件）   A B+ C  必须满足这个模式 A B 任意事件*(但是其中B可以随意组合) B C
 *    容忍跨匹配到的事件进行匹配  比如  A B1 B4 C
 *    eg:  A D1 B1 D1 B2 D2 B3 B4 C -->  A B1 B2 B3 B4 C
 *                                    A B1 B2 B4 C
 *                                    A B1 B3 B4 C
 *                                    A B1 B4 C
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/14
 */
public class CombiningPatternsExample {
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
                // default Relaxed Contiguity
                .oneOrMore()
                // strict contiguity
//                .consecutive()
                // non-deterministic relaxed contiguity
//                .allowCombinations()
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
        env.execute("CombiningPatternsExample");
    }
}
