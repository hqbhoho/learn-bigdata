package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import com.hqbhoho.bigdata.learnFlink.streaming.stateAndCheckpoint.BroadcastStateExample;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

import java.util.Arrays;

/**
 * describe:
 * Connect coMap Example
 * <p>
 * Test:
 * nc -l 19999
 * hqbhoho,300
 * hb,800
 * nc -l 29999
 * hqbhih,300
 * hqbhoho,200
 * Result：
 * (0,300)
 * (300,300)
 * (1100,300)
 * (1100,500)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/13
 */
public class ConnectExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        int port2 = tool.getInt("port2", 29999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 模拟数据源
        DataStream<Tuple2<String, String>> input1 = env.socketTextStream(host, port1)
                .map(new BroadcastStateExample.String2Tuple2MapperFuncation(","));
        DataStream<Tuple2<String, String>> input2 = env.socketTextStream(host, port2)
                .map(new BroadcastStateExample.String2Tuple2MapperFuncation(","));
        input1.connect(input2)
                .map(new MyCoMapFunction())
                .print();

        env.execute("ConnectExample");
    }

    /**
     * 处理具体实现，统计两个流中总sum值
     */
    static class MyCoMapFunction extends RichCoMapFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<Integer, Integer>> implements CheckpointedFunction {

        private ListState<Tuple2<Integer, Integer>> countState;

        private Tuple2<Integer, Integer> count;

        @Override
        public void open(Configuration parameters) throws Exception {
            count = Tuple2.of(0, 0);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            countState.update(Arrays.asList(count));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<Integer, Integer>> countStateDescriptor = new ListStateDescriptor<Tuple2<Integer, Integer>>(
                    "countStateDescriptor",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                    })
            );
            countState = context.getOperatorStateStore().getListState(countStateDescriptor);
            if (context.isRestored()) {
                count = countState.get().iterator().next();
            }
        }

        @Override
        public Tuple2<Integer, Integer> map1(Tuple2<String, String> value) throws Exception {
            count.f0 += Integer.valueOf(value.f1);
            return count;
        }

        @Override
        public Tuple2<Integer, Integer> map2(Tuple2<String, String> value) throws Exception {
            count.f1 += Integer.valueOf(value.f1);
            return count;
        }
    }
}
