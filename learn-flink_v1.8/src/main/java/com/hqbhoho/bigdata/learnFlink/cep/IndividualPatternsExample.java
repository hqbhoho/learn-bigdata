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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * describe:
 * xiaoxiao,111,2,1557109591000
 * hqbhoho,240,2,1557109592000
 * hqbhoho,240,4,1557109593000
 * hqbhoho,112,2,1557109594000
 * hqbhoho,114,4,1557109595000
 * xiaoxiao,111,2,1557109596000
 * results:
 * <p>
 * greedy()
 * {A=[(hqbhoho,240,4,1557109593000)]}
 * ----------------------------------------------
 * {A=[(hqbhoho,240,4,1557109593000), (hqbhoho,112,2,1557109594000)]}
 * {A=[(hqbhoho,112,2,1557109594000)]}
 * ---------------------------------------------------
 * {A=[(hqbhoho,240,4,1557109593000), (hqbhoho,112,2,1557109594000), (hqbhoho,114,4,1557109595000)]}
 * {A=[(hqbhoho,112,2,1557109594000), (hqbhoho,114,4,1557109595000)]}
 * {A=[(hqbhoho,114,4,1557109595000)]}
 * <p>
 * //.greedy();  感觉没有什么用
 * {A=[(hqbhoho,240,4,1557109593000)]}
 * --------------------------------------------------------------
 * {A=[(hqbhoho,240,4,1557109593000), (hqbhoho,112,2,1557109594000)]}
 * {A=[(hqbhoho,112,2,1557109594000)]}
 * ----------------------------------------------------------
 * {A=[(hqbhoho,240,4,1557109593000), (hqbhoho,112,2,1557109594000), (hqbhoho,114,4,1557109595000)]}
 * {A=[(hqbhoho,112,2,1557109594000), (hqbhoho,114,4,1557109595000)]}
 * {A=[(hqbhoho,114,4,1557109595000)]}
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/14
 */
public class IndividualPatternsExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
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

        patternStream.process(new PatternProcessFunction<Tuple4<String, BigDecimal, BigDecimal, Long>, Tuple4<String, BigDecimal, BigDecimal, Long>>() {
            @Override
            public void processMatch(Map<String, List<Tuple4<String, BigDecimal, BigDecimal, Long>>> map, Context context, Collector<Tuple4<String, BigDecimal, BigDecimal, Long>> collector) throws Exception {
                Optional.ofNullable(map).ifPresent(System.out::println);
            }
        });
        env.execute("IndividualPatternsExample");
    }
}
