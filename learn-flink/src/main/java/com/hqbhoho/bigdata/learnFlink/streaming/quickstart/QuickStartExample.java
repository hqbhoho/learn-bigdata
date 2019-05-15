package com.hqbhoho.bigdata.learnFlink.streaming.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * describe:
 * Flink Streaming Example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/18
 */
public class QuickStartExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("10.105.1.182", 19999)
                .flatMap(new FlatMapFunction<String,Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Arrays.asList(s.split(" "))
                                .stream()
                                .map(word -> new Tuple2<String, Integer>(word, 1))
                                .forEach(collector::collect);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        dataStream.print();

        env.execute("Window WordCount");
    }
}
