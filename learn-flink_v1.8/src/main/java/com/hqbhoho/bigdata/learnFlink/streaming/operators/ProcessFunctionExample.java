package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

/**
 * describe:
 * The ProcessFunction is a low-level stream processing operation
 * <p>
 * 实现类似窗口的统计操作  根据消息的eventtime    生成依赖eventtime的固定大小的窗口。
 * <p>
 * nc -l 19999
 * hqbhoho,100,1557109591000          56,watermater generate： 1557109591000    生成窗口  [1557109591000,1557109594000]
 * hqbhoho,200,1557109592000          56,watermater generate： 1557109592000
 * hqbhoho,300,1557109593000          56,watermater generate： 1557109593000
 * hqbhoho,400,1557109594000          56,watermater generate： 1557109594000    trigger window function
 * hqbhoho,600,1557109596000          56,watermater generate： 1557109596000    生成窗口  [1557109596000,1557109599000]
 * hqbhoho,800,1557109597000          56,watermater generate： 1557109597000
 * hqbhoho,900,1557109599000          56,watermater generate： 1557109599000    trigger window function
 * <p>
 * results:
 * 56,watermater generate： 1557109593000
 * 56,watermater generate： 1557109594000
 * (hqbhoho,4,1000,1557109594000)
 * 56,watermater generate： 1557109597000
 * 56,watermater generate： 1557109599000
 * (hqbhoho,3,2300,1557109599000)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/14
 */
public class ProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        // 获取数据源
        env.socketTextStream(host, port1)
                .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String line) {
                        String[] list = line.split(",");
                        return new Tuple3<String, Integer, Long>(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
                    }
                })
                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks())
                .keyBy(t -> t.f0)
                .process(new myKeyedProcessFunction())
                .print();
        env.execute("ProcessFunctionExample");
    }

    /**
     * customer KeyedProcessFunction
     */
    static class myKeyedProcessFunction extends KeyedProcessFunction<String, Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Long>> {

        private ValueState<Tuple2<Integer, Integer>> countState;

        private ListState<Long> registerTimeStampListState;

        /**
         * init state
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Integer, Integer>> countStateDescriptor = new ValueStateDescriptor<>(
                    "countStateDescriptor",
                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                    })
            );
            ListStateDescriptor<Long> registerTimeStampListStateDescriptor = new ListStateDescriptor<>(
                    "registerTimeStampListStateDescriptor",
                    BasicTypeInfo.LONG_TYPE_INFO
            );
            countState = getRuntimeContext().getState(countStateDescriptor);
            registerTimeStampListState = getRuntimeContext().getListState(registerTimeStampListStateDescriptor);
        }

        @Override
        public void processElement(Tuple3<String, Integer, Long> value, Context ctx, Collector<Tuple4<String, Integer, Integer, Long>> out) throws Exception {
            Tuple2<Integer, Integer> count = countState.value();
            if (count == null) {
                count = Tuple2.of(0, 0);
            }
            Long registerTimestamp = value.f2 + 3000;
            countState.update(Tuple2.of(count.f0 + 1, count.f1 + value.f1));
            Iterable<Long> timeStampList = registerTimeStampListState.get();
            if (timeStampList == null) {
                registerTimeStampListState.addAll(Arrays.asList(registerTimestamp));
                ctx.timerService().registerEventTimeTimer(value.f2 + 3000);
            } else {
                Iterator<Long> it = timeStampList.iterator();
                Long max = 0L;
                while (it.hasNext()) {
                    max = Math.max(it.next(), max);
                }
                if (registerTimestamp > max + 3000) {
                    registerTimeStampListState.addAll(Arrays.asList(registerTimestamp));
                    ctx.timerService().registerEventTimeTimer(value.f2 + 3000);
                }
            }
        }

        /**
         *   when watermark >= timer.gettimestamp(ctx.timerService().registerEventTimeTimer(value.f2 + 3000))
         *   this method will be invoked.
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, Integer, Integer, Long>> out) throws Exception {
            Tuple2<Integer, Integer> value = countState.value();
            out.collect(Tuple4.of(ctx.getCurrentKey(), value.f0, value.f1, ctx.timestamp()));
            countState.clear();
        }
    }
}
