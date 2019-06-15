package com.hqbhoho.bigdata.learnFlink.cep;

import com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream.DetectingPatternsInTablesExample;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
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
 * 模式匹配组   类似于正则表达式中的group的概念
 * Test1： .followedByAny("A2") + .oneOrMore()   模式（A1 A2+）出现一次或多次
 * xiaoxiao,111,2,1557109591000       1
 * hqbhoho,241,2,1557109592000        2
 * hqbhoho,112,2,1557109593000        3
 * hqbhoho,242,4,1557109594000        4
 * hqbhoho,113,2,1557109595000        5
 * hqbhoho,243,4,1557109596000        6
 * hqbhoho,114,4,1557109597000        7
 * hqbhoho,245,5,1557109598000        8
 * xixi,129,2,1557109598001           9
 * xiaoxiao,118,2,1557109599000       10
 * results:
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,242,4,1557109594000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000)], B1=[(hqbhoho,242,4,1557109594000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,113,2,1557109595000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * 小结：
 * 以 第一个匹配是(hqbhoho,241,2,1557109592000)为例讲解
 * 1. A1：2   A2:3 5 7                             1
 * 2. A1:2    A2:3 5     A1:6  A2:7                2
 * 3. A1:2    A2:5 7                               1
 * 4. A1:2    A2:3      A1:4  A2:5 7               2
 * 5. A1:2    A2:3      A1:4  A2:5     A1:6 A2:7    3
 * 6. A1:2    A2:3      A1:4  A2:7                 2
 * 7. A1:2    A2:5      A1:6  A2:7                  2
 * 8. A1:2    A2:7                                  1
 * 1+2+1+2+3+2+2+1 = 14   (A1:2    A2:3) 这种情况重复两次  所以  14-2 =12种
 * <p>
 * <p>
 * Test2: .followedByAny("A2")     模式（A1 A2+）只出现一次
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000)], B1=[(hqbhoho,242,4,1557109594000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,113,2,1557109595000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * 小结：
 * 以 第一个匹配是(hqbhoho,241,2,1557109592000)为例讲解
 * 1. A1：2   A2:3 5 7
 * 2. A1:2    A2:3 5
 * 3. A1:2    A2:5 7
 * 4. A1:2    A2:3
 * 5. A1:2    A2:5
 * 6. A1:2    A2:7
 * 所以  6种
 * <p>
 * <p>
 * Test3: .followedBy("A2") + .oneOrMore()  模式（A1 A2+）出现一次或多次
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,242,4,1557109594000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000), (hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000)], B1=[(hqbhoho,242,4,1557109594000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000), (hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,113,2,1557109595000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * 小结：
 * 以 第一个匹配是(hqbhoho,241,2,1557109592000)为例讲解
 * 1. A1：2   A2:3 5 7                             1
 * 2. A1:2    A2:3 5     A1:6  A2:7                2
 * 4. A1:2    A2:3      A1:4  A2:5 7               2
 * 5. A1:2    A2:3      A1:4  A2:5     A1:6 A2:7    3
 * 6. A1:2    A2:3                                  1
 * 1+2+2+3+1 = 9   (A1:2    A2:3) 这种情况重复两次  所以  9-2 =7种
 * <p>
 * <p>
 * Test4: .followedBy("A2")   模式（A1 A2+）出现一次
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000), (hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,241,2,1557109592000)], A2=[(hqbhoho,112,2,1557109593000)], B1=[(hqbhoho,242,4,1557109594000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,112,2,1557109593000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,113,2,1557109595000), (hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,242,4,1557109594000)], A2=[(hqbhoho,113,2,1557109595000)], B1=[(hqbhoho,243,4,1557109596000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,113,2,1557109595000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * {A1=[(hqbhoho,243,4,1557109596000)], A2=[(hqbhoho,114,4,1557109597000)], B1=[(hqbhoho,245,5,1557109598000)], B2=[(xiaoxiao,118,2,1557109599000)]}
 * 小结：
 * 以 第一个匹配是(hqbhoho,241,2,1557109592000)为例讲解
 * 1. A1：2   A2:3 5 7                             1
 * 2. A1:2    A2:3 5                               1
 * 4. A1:2    A2:3                                 1
 * 所以3种
 * 总结：
 * followedBy  比  followedByAny   两者是讲究event的顺序的  不同之处在于  followedBy不能跳过匹配到的事件，而followedByAny可以跳过匹配到的事件进行组合
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/14
 */
public class GroupsPatternsExample {
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
        // 定义模式 A  A1 A2+
        Pattern A = Pattern.<Tuple4<String, BigDecimal, BigDecimal, Long>>begin("A1")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f0.contains("hqb");
                    }
                })
                // Test
//                .followedByAny("A2")
                // Test
                .followedBy("A2")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f1.intValue() - 200 < 0;
                    }
                })
                .oneOrMore();
        // 定义模式B B1
        Pattern B = Pattern.<Tuple4<String, BigDecimal, BigDecimal, Long>>begin("B1")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f2.intValue() > 3;
                    }
                })
                .followedBy("B2")
                .where(new SimpleCondition<Tuple4<String, BigDecimal, BigDecimal, Long>>() {
                    @Override
                    public boolean filter(Tuple4<String, BigDecimal, BigDecimal, Long> value) throws Exception {
                        return value.f0.contains("xiao");
                    }
                });
        // 定义 Parrern group  (A1 A2+)+ (B1 B2)
        GroupPattern pattern = Pattern.begin(A)
                // Test
//                .oneOrMore()
                // Test
                .next(B);

        PatternStream<Tuple4<String, BigDecimal, BigDecimal, Long>> patternStream
                = CEP.pattern(input, pattern);
        patternStream.process(new PatternProcessFunction<Tuple4<String, BigDecimal, BigDecimal, Long>, Tuple4<String, BigDecimal, BigDecimal, Long>>() {
            @Override
            public void processMatch(Map<String, List<Tuple4<String, BigDecimal, BigDecimal, Long>>> map, Context context, Collector<Tuple4<String, BigDecimal, BigDecimal, Long>> collector) throws Exception {
                Optional.ofNullable(map).ifPresent(System.out::println);
            }
        });
        env.execute("GroupsPatternsExample");
    }
}
