package com.hqbhoho.bigdata.learnFlink.streaming.StateAndCheckpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * describe:
 * Broadcast State Example
 * <p>
 * <p>
 * 测试：
 * nc -l 19999
 * hqbhoho,200
 * hqbhoho,200
 * hqbhoho,300
 * nc -l 29999
 * hqbhoho,make,23
 * hqbhoho,male,24
 * 输出
 * 2> (hqbhoho,null,null,1,200)
 * 2> (hqbhoho,make,23,2,400)
 * 2> (hqbhoho,male,24,3,700)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/09
 */
public class BroadcastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启重试策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));

        // 事件流
        DataStream<Tuple2<String, String>> eventStream = env.socketTextStream("10.105.1.182", 19999)
                .map(new String2Tuple2MapperFuncation(","));
        // 配置流
        DataStream<Tuple3<String, String, String>> configStream = env.socketTextStream("10.105.1.182", 29999)
                .map(new String2Tuple3MapperFuncation(","));

        // 创建BroadcastState
        MapStateDescriptor<String, Tuple3<String, String, String>> configMapState = new MapStateDescriptor(
                "configMapState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
                })
        );

        BroadcastStream<Tuple3<String, String, String>> broadcastConfigStream = configStream.broadcast(configMapState);

        // 流之间关联操作
        eventStream.keyBy(0).connect(broadcastConfigStream)
                .process(new KeyedBroadcastProcessFunction<String, Tuple2<String, String>, Tuple3<String, String, String>, Tuple5<String, String, String, Integer, Integer>>() {

                    private MapState<String, Tuple3<String, Integer, Integer>> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 累计值状态
                        MapStateDescriptor<String, Tuple3<String, Integer, Integer>> countMapStateDescriptor = new MapStateDescriptor(
                                "countMapState",
                                BasicTypeInfo.STRING_TYPE_INFO,
                                TypeInformation.of(new TypeHint<Tuple3<String, Integer, Integer>>() {
                                })
                        );
                        countState = getRuntimeContext().getMapState(countMapStateDescriptor);
                    }

                    // 处理eventStream
                    @Override
                    public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple5<String, String, String, Integer, Integer>> out) throws Exception {
                        Tuple3<String, Integer, Integer> count = countState.get(value.f0);
                        if (count == null) {
                            count = Tuple3.of(value.f0, 0, 0);
                        }
                        Tuple3<String, String, String> config = ctx.getBroadcastState(configMapState).get(value.f0);
                        if (config == null) {
                            config = Tuple3.of(null, null, null);
                        }
                        countState.put(value.f0, Tuple3.of(value.f0, count.f1 + 1, count.f2 + Integer.valueOf(value.f1)));
                        out.collect(Tuple5.of(value.f0, config.f1, config.f2, count.f1 + 1, count.f2 + Integer.valueOf(value.f1)));
                    }

                    // 处理configStream
                    @Override
                    public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple5<String, String, String, Integer, Integer>> out) throws Exception {
                        // 更新broadcast state
                        ctx.getBroadcastState(configMapState).put(value.f0, value);

                    }
                });

        env.execute("BroadcastStateExample");
    }

    /**
     * String -> Tuple2
     */
    static class String2Tuple2MapperFuncation implements MapFunction<String, Tuple2<String, String>> {

        private String delimiter;

        public String2Tuple2MapperFuncation(String delimiter) {
            this.delimiter = delimiter;
        }

        @Override
        public Tuple2<String, String> map(String value) throws Exception {

            String[] list = value.split(delimiter);
            return Tuple2.of(list[0], list[1]);
        }
    }

    /**
     * String -> Tuple3
     */
    static class String2Tuple3MapperFuncation implements MapFunction<String, Tuple3<String, String, String>> {

        private String delimiter;

        public String2Tuple3MapperFuncation(String delimiter) {
            this.delimiter = delimiter;
        }

        @Override
        public Tuple3<String, String, String> map(String value) throws Exception {

            String[] list = value.split(delimiter);
            return Tuple3.of(list[0], list[1], list[2]);
        }
    }
}
