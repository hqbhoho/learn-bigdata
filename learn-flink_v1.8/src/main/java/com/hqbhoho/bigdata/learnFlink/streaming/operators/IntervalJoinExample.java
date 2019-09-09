package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * describe:
 * Interval Join Example (KeyedStream,KeyedStream → DataStream)
 * input1.timestamp + lowerBound <= input2.timestamp <= input1.timestamp + upperBound
 * <p>
 * <p>
 * 测试：
 * nc -l 19999
 * hqbhoho,100,1557109591000
 * input1 event timestamp :1557109591000      input2 event timestamp :(1557109589000,1557109592000] will be interval join
 * nc -l 29999
 * hqbhoho,1,1557109588000
 * (console output: no results)
 * hqbhoho,2,1557109589000
 * (console output: no results)
 * hqbhoho,3,1557109590000
 * console output:
 * LeftTimestamp: 1557109591000,RightTimestamp: 1557109590000,Timestamp: 1557109591000
 * 2> (hqbhoho,100,3)
 * hqbhoho,4,1557109591000
 * console output:
 * LeftTimestamp: 1557109591000,RightTimestamp: 1557109591000,Timestamp: 1557109591000
 * 2> (hqbhoho,100,4)
 * hqbhoho,5,1557109592000
 * console output:
 * LeftTimestamp: 1557109591000,RightTimestamp: 1557109592000,Timestamp: 1557109592000
 * 2> (hqbhoho,100,5)
 * hqbhoho,6,1557109593000
 * (console output: no results)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/10
 */
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        int port2 = tool.getInt("port2", 29999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //配置运行参数
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10000000);
        // 获取数据
        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input1 = env.socketTextStream(host, port1)
                .map(new WindowJoinExample.TokenFunction())
                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks())
                .keyBy(0);
        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input2 = env.socketTextStream(host, port2)
                .map(new WindowJoinExample.TokenFunction())
                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks())
                .keyBy(0);
        //interval join
        // input1.timestamp -2 < input2.timestamp <= input1.timestamp + 1
        input1.intervalJoin(input2)
                .between(Time.seconds(-2), Time.seconds(1))
                .lowerBoundExclusive()
                .process(new ProcessJoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void processElement(Tuple3<String, Integer, Long> left, Tuple3<String, Integer, Long> right, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        Optional.ofNullable("LeftTimestamp: " + ctx.getLeftTimestamp() + ",RightTimestamp: " + ctx.getRightTimestamp() + ",Timestamp: " + ctx.getTimestamp())
                                .ifPresent(System.out::println);
                        out.collect(Tuple3.of(left.f0, left.f1, right.f1));
                    }
                }).print();
        env.execute("IntervalJoinExample");
    }
}
