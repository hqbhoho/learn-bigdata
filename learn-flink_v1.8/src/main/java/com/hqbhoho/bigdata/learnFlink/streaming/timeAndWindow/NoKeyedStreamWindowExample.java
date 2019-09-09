package com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow;

import com.hqbhoho.bigdata.learnFlink.streaming.stateAndCheckpoint.BroadcastStateExample;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

/**
 * describe:
 * <p>
 * Keyed Stream  ,Window Compute can be in parallel, All elements referring to the same key will be sent to the same parallel task.
 * No Keyed Stream , Window Compute in only one task, parallel=1。
 * <p>
 * start log:
 * Source: Socket Stream (1/1) (1d77f4f6b72b918396e2025724fa6b71) switched from SCHEDULED to DEPLOYING.
 * Deploying Source: Socket Stream (1/1) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * Map (1/4) (431bf0ba59a4453d09271e05782bd33a) switched from SCHEDULED to DEPLOYING.
 * Deploying Map (1/4) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * Map (2/4) (488ff5e699d395fcb5ee148502283226) switched from SCHEDULED to DEPLOYING.
 * Deploying Map (2/4) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * Map (3/4) (21969a571194572967fcdc5a9bde5995) switched from SCHEDULED to DEPLOYING.
 * Deploying Map (3/4) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * Map (4/4) (e82b7978587bb431f9347dbd489b5c87) switched from SCHEDULED to DEPLOYING.
 * Deploying Map (4/4) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * TriggerWindow(TumblingProcessingTimeWindows(10000), ListStateDescriptor{name=window-contents, defaultValue=null, serializer=org.apache.flink.api.common.typeutils.base.ListSerializer@d6a7b0f0}, ProcessingTimeTrigger(), com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.NoKeyedStreamWindowExample$2@ea1a8d5, AllWindowedStream.main(NoKeyedStreamWindowExample.java:81)) (1/1) (c87ed2caac62ad0560f61516654ba024) switched from SCHEDULED to DEPLOYING.
 * Deploying TriggerWindow(TumblingProcessingTimeWindows(10000), ListStateDescriptor{name=window-contents, defaultValue=null, serializer=org.apache.flink.api.common.typeutils.base.ListSerializer@d6a7b0f0}, ProcessingTimeTrigger(), com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.NoKeyedStreamWindowExample$2@ea1a8d5, AllWindowedStream.main(NoKeyedStreamWindowExample.java:81)) (1/1) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * Sink: Print to Std. Out (1/4) (55d2990a4e2b3db35bae72f02b38d8cf) switched from SCHEDULED to DEPLOYING.
 * Deploying Sink: Print to Std. Out (1/4) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * Sink: Print to Std. Out (2/4) (1c5678658df2a29a2835a5850b9d7c35) switched from SCHEDULED to DEPLOYING.
 * Deploying Sink: Print to Std. Out (2/4) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * Sink: Print to Std. Out (3/4) (ea2116c740c1eb310570666bcd34ebac) switched from SCHEDULED to DEPLOYING.
 * Deploying Sink: Print to Std. Out (3/4) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * Sink: Print to Std. Out (4/4) (1f2e0d720886f36afd24bb5b0da76ba8) switched from SCHEDULED to DEPLOYING.
 * Deploying Sink: Print to Std. Out (4/4) (attempt #0) to f7705531-f184-4959-b5fe-20b1c6a6a7bd @ 127.0.0.1 (dataPort=-1)
 * <p>
 * parallel
 * Source : 1
 * Map : 4
 * TriggerWindow : 1
 * Sink : 1
 * <p>
 * Test:
 * nc -l 19999
 * hqbhoho,200
 * xiaomi,300
 * xx,200
 * Results:
 * first step: evictBefore() invoked
 * Evictor(Before) Thread ID: 69,Process Window:[2019-05-13 19:59:00,2019-05-13 19:59:10]
 * original data size: 3
 * hqb data size: 1
 * second step: window function invoked
 * Window Funcation Thread ID: 69,Process Window:[2019-05-13 19:59:00,2019-05-13 19:59:10]
 * last step: evictAfter() invoked
 * Evictor(After) Thread ID: 69
 * event value: [hqbhoho,200]
 * Window output: 200
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/13
 */
public class NoKeyedStreamWindowExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // 模拟数据源
        DataStream<Tuple2<String, String>> input1 = env.socketTextStream(host, port1)
                .map(new BroadcastStateExample.String2Tuple2MapperFuncation(","));
        input1.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                // before or after window func , invoke
                .evictor(new Evictor<Tuple2<String, String>, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple2<String, String>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Optional.ofNullable("Evictor(Before) Thread ID: " + Thread.currentThread().getId() + ",Process Window:[" + sdf.format(new Date(window.getStart())) + "," + sdf.format(new Date(window.getEnd())) + "]")
                                .ifPresent(System.out::println);
                        Optional.ofNullable("original data size: " + size).ifPresent(System.out::println);
                        Iterator<TimestampedValue<Tuple2<String, String>>> it = elements.iterator();
                        while (it.hasNext()) {
                            //filter event which f0 not contain "hqb"
                            if (!it.next().getValue().f0.contains("hqb")) {
                                it.remove();
                                size--;
                            }
                        }
                        Optional.ofNullable("hqb data size: " + size).ifPresent(System.out::println);
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple2<String, String>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                        Iterator<TimestampedValue<Tuple2<String, String>>> it = elements.iterator();
                        Optional.ofNullable("Evictor(After) Thread ID: " + Thread.currentThread().getId()).ifPresent(System.out::println);
                        while (it.hasNext()) {
                            TimestampedValue<Tuple2<String, String>> next = it.next();
                            Optional.ofNullable("event value: [" + next.getValue().f0 + "," + next.getValue().f1 + "]")
                                    .ifPresent(System.out::println);
                        }

                    }
                })
                .apply(new AllWindowFunction<Tuple2<String, String>, Integer, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<String, String>> values, Collector<Integer> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Optional.ofNullable("Window Funcation Thread ID: " + Thread.currentThread().getId() + ",Process Window:[" + sdf.format(new Date(window.getStart())) + "," + sdf.format(new Date(window.getEnd())) + "]")
                                .ifPresent(System.out::println);
                        int count = 0;
                        Iterator<Tuple2<String, String>> it = values.iterator();
                        while (it.hasNext()) {
                            count += Integer.valueOf(it.next().f1);
                        }
                        out.collect(count);
                    }
                })
                .print();
        env.execute("NoKeyedStreamWindowExample");
    }
}
