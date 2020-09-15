package com.hqbhoho.bigdata.flink.stream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/26
 */
public class StreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置checkpoint的参数
        env.enableCheckpointing(10000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(5000L);
        checkpointConfig.setCheckpointTimeout(5000L);
        checkpointConfig.setFailOnCheckpointingErrors(true);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //设置state backend
        StateBackend backend = new FsStateBackend(
                "file:/E:/flink_test/checkpoint",
                false);
        env.setStateBackend(backend);

        env.addSource(new Source())
                .flatMap(new OperatorStateMapFunc()).uid("operator_state")
                .keyBy(t -> t.f0)
                .process(new KeyedStateProcessFunc()).uid("keyed_state")
                .print();

        env.execute("StateExample");
    }


    /**
     * keyed state example
     */
    static class KeyedStateProcessFunc extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> valueState =
                    new ValueStateDescriptor<Integer>("value", Types.INT, 0);
            countState = getRuntimeContext().getState(valueState);
        }

        @Override
        public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
            int count = countState.value();
            countState.update(++count);
            collector.collect(Tuple2.of(context.getCurrentKey(), count));

        }
    }

    /**
     * operator state example
     */
    static class OperatorStateMapFunc extends RichFlatMapFunction<String, Tuple2<String, Integer>> implements CheckpointedFunction {

        private transient ListState<Integer> checkPointCountList;

        private Integer count;

        @Override
        public void open(Configuration parameters) throws Exception {
            count = 0;
        }

        @Override
        public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
            count++;
            Optional.ofNullable("<====lines====>" + count).ifPresent(System.out::println);
            Stream.of(input.split(",")).forEach(word -> collector.collect(Tuple2.of(word, 1)));
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
            ListStateDescriptor<Integer> listStateDescriptor =
                    new ListStateDescriptor<Integer>("checkPointCountList", Types.INT);
            checkPointCountList = context.getOperatorStateStore().getListState(listStateDescriptor);
            // 如果是失败恢复的，从状态中恢复数据
            if (context.isRestored()) {
                System.out.println("<======isRestored=====>");
                count = checkPointCountList.get().iterator().next();
            }
        }
    }
}
