package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 * Split Stream and Select one
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/13
 */
public class SplitAndSelectExample {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 模拟数据源
        DataStreamSource<Integer> input = env.fromElements(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        SplitStream<Integer> split = input.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;

            }
        });
        // 偶数流
        split.select("even").map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer value) throws Exception {
                return "even: "+value;
            }
        }).print();

        // 奇数流
        split.select("odd").map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer value) throws Exception {
                return "odd: "+value;
            }
        }).print();

        env.execute("SplitAndSelectExample");
    }
}
