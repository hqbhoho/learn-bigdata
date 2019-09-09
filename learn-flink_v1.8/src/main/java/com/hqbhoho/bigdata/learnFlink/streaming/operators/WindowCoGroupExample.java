package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Optional;

/**
 * describe:
 * Window CoGroup Example
 * <p>
 * Test:
 * nc -l 19999
 * hqbhoho,100,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,300,1557109593000
 * nc -l 29999
 * hqbhoho,1,1557109591000
 * hqbhoho,2,1557109592000
 * hqbhoho,3,1557109593000
 * result:
 * 59,watermater generate： 1557109593000
 * 58,watermater generate： 1557109592000
 * 59,watermater generate： 1557109593000
 * 58,watermater generate： 1557109593000
 * First Stream Window events:
 * (hqbhoho,100,1557109591000)
 * (hqbhoho,200,1557109592000)
 * Second Stream Window events:
 * (hqbhoho,1,1557109591000)
 * (hqbhoho,2,1557109592000)
 * (hqbhoho,300,3)
 * 59,watermater generate： 1557109593000
 * 58,watermater generate： 1557109593000
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/13
 */
public class WindowCoGroupExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        int port2 = tool.getInt("port2", 29999);
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置运行环境
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        // 获取数据
        DataStream<Tuple3<String, Integer, Long>> input1 = env.socketTextStream(host, port1)
                .map(new WindowJoinExample.TokenFunction())
                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks());
        DataStream<Tuple3<String, Integer, Long>> input2 = env.socketTextStream(host, port2)
                .map(new WindowJoinExample.TokenFunction())
                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks());

        input1.coGroup(input2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new CoGroupFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, Integer, Long>> first, Iterable<Tuple3<String, Integer, Long>> second, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        Optional.ofNullable("First Stream Window events:").ifPresent(System.out::println);
                        first.forEach(System.out::println);
                        Optional.ofNullable("Second Stream Window events:").ifPresent(System.out::println);
                        second.forEach(System.out::println);
                        int count1 = 0;
                        int count2 = 0;
                        String key = null;
                        Iterator<Tuple3<String, Integer, Long>> it1 = first.iterator();
                        Iterator<Tuple3<String, Integer, Long>> it2 = second.iterator();

                        while (it1.hasNext()) {
                            Tuple3<String, Integer, Long> next = it1.next();
                            if (key == null) {
                                key = next.f0;
                            }
                            count1 += next.f1;
                        }
                        while (it2.hasNext()) {
                            count2 += it2.next().f1;
                        }
                        out.collect(Tuple3.of(key, count1, count2));
                    }
                })
                .print();
        env.execute("WindowCoGroupExample");
    }
}
