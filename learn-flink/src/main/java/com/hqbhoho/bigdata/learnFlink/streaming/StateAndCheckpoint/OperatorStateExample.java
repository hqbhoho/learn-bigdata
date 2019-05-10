package com.hqbhoho.bigdata.learnFlink.streaming.StateAndCheckpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Optional;

/**
 * describe:
 * Operator State Example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/07
 */
public class OperatorStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置checkpoint的参数
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
        //设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                3, // number of restart attempts
//                Time.of(10, TimeUnit.SECONDS) // delay
//        ));
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 模拟数据源
//        DataStreamSource<Tuple3<Long, String, Integer>> input = env.addSource(new MyDataSource());
        DataStreamSource<String> input = env.socketTextStream("192.168.5.131", 19999);

        input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] list = line.split(",");
                return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
            }
        }).map(new countCompute()).print();
        env.execute("OperatorStateExample");
    }

    /**
     * operator state example
     */
    static class countCompute extends RichMapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>> implements CheckpointedFunction {

        private transient ListState<Tuple2<Integer, Integer>> checkPointCountList;

        private Tuple2<Integer, Integer> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            count = Tuple2.of(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple2<String, Integer> input) throws Exception {
            count = Tuple2.of(count.f0 + 1, count.f1 + input.f1 );
            Optional.ofNullable("当前线程：" + Thread.currentThread().getId() +
                    ";处理消息：(" + input.f0 + "," + input.f1 +
                    "),旧状态值为：(" + count.f0 + "," + count.f1 + ")").ifPresent(System.out::println);
            return count;
        }

        /**
         * when checkpoint, this method will be invoked
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkPointCountList.update(Arrays.asList(count));
        }

        /**
         * operator state init , only be invoked once.
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<Integer, Integer>> listStateDescriptor =
                    new ListStateDescriptor<Tuple2<Integer, Integer>>("checkPointCountList", TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                    }));
            checkPointCountList = context.getOperatorStateStore().getListState(listStateDescriptor);
            // 如果是失败恢复的，从状态中恢复数据
            if (context.isRestored()) {
                count = checkPointCountList.get().iterator().next();
            }
        }
    }
}
