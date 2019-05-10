package com.hqbhoho.bigdata.learnFlink.streaming.StateAndCheckpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 * State operate example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/07
 */
public class KeyedStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置checkpoint的参数
        // 开启checkpoint
        env.enableCheckpointing(10000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(5000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setFailOnCheckpointingErrors(true);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //设置state backend
        StateBackend backend = new FsStateBackend(
                "file:/E:/flink_test/checkpoint",
                false);
        env.setStateBackend(backend);

        // 开启重试策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));
        // 模拟数据源
//        DataStreamSource<Tuple2<String, Integer>> input = env.fromElements(
//                Tuple2.of("hqbhoho01", 200),
//                Tuple2.of("hqbhoho01", 300),
//                Tuple2.of("hqbhoho01", 400),
//                Tuple2.of("hqbhoho01", 500),
//                Tuple2.of("xhbhoho01", 500),
//                Tuple2.of("xhbhoho01", 600));

        DataStreamSource<String> input = env.socketTextStream("10.105.1.182", 19999);

        input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] list = line.split(",");
                return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
            }
        }).keyBy(0).map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>>() {

            private transient ListState<Tuple2<Integer, Integer>> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 状态过期设置(只针对keyed state     checkpoint中的state也会失效)
//                StateTtlConfig ttlConfig = StateTtlConfig
//                        .newBuilder(Time.seconds(10))
//                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                        .build();
                // 创建一个状态描述
                ListStateDescriptor<Tuple2<Integer, Integer>> countStateDescriptor =
                        new ListStateDescriptor<Tuple2<Integer, Integer>>(
                                "countStateDescriptor",
                                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                                })
                        );
//                countStateDescriptor.enableTimeToLive(ttlConfig);
                // 获取一个状态
                countState = getRuntimeContext().getListState(countStateDescriptor);
            }

            @Override
            public Tuple2<Integer, Integer> map(Tuple2<String, Integer> input) throws Exception {
                Iterable<Tuple2<Integer, Integer>> it = countState.get();
                Tuple2<Integer, Integer> oldCount = Tuple2.of(0, 0);
                if (it.iterator().hasNext()) {
                    oldCount = it.iterator().next();
                }
                Tuple2<Integer, Integer> newCount = Tuple2.of(oldCount.f0 + 1, oldCount.f1 + input.f1);
                countState.update(Arrays.asList(newCount));
                Optional.ofNullable("当前线程：" + Thread.currentThread().getId() + ";处理消息：(" + input.f0 + "," + input.f1 + "),旧状态值为：(" + oldCount.f0 + "," + oldCount.f1 + ")").ifPresent(System.out::println);
                return newCount;
            }
        }).print();

        env.execute("KeyedStateExample");
    }
}
