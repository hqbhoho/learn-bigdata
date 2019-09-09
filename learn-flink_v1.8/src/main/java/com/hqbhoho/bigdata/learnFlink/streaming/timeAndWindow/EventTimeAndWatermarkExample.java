package com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

/**
 * describe:
 * event time and watermark example, how to process late event
 * <p>
 * ****************************************************************************************
 * 测试场景 1
 * nc -lk 19999
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109593000
 * hqbhoho,200,1557109594000
 * hqbhoho,200,1557109595000
 * 结果：
 * watermark generate, watermark: 1557109592000
 * watermark generate, watermark: 1557109593000
 * WinodwFuncation invoked, Window:[1557109590000,1557109593000]
 * WinodwFuncation invoked, Window:[2019-05-06 10:26:30,2019-05-06 10:26:33] (3s window first event timestamp:2019-05-06 10:26:31)
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * (hqbhoho,400,1557109590000)
 * watermark generate, watermark: 1557109593000
 * Window 时间范围  左闭右开  watermark >= window end_time  trigger window operation
 * ****************************************************************************************
 * <p>
 * 测试场景 2
 * nc -lk 19999
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109595000
 * hqbhoho,200,1557109591000
 * 结果：
 * watermark generate, watermark: 1557109590000
 * watermark generate, watermark: 1557109593000
 * WinodwFuncation invoked, Window:[1557109590000,1557109593000]
 * WinodwFuncation invoked, Window:[2019-05-06 10:26:30,2019-05-06 10:26:33]
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109591000
 * (hqbhoho,800,1557109590000)
 * watermark generate, watermark: 1557109593000
 * 最后一条event 被丢弃
 * ****************************************************************************************
 * <p>
 * 测试场景 3 (Window allowedLateness)
 * nc -lk 19999
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109595000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109595000
 * hqbhoho,200,1557109596000
 * hqbhoho,200,1557109591000
 * 结果：
 * watermark generate, watermark: 1557109590000
 * watermark generate, watermark: 1557109593000
 * WinodwFuncation invoked, Window:[1557109590000,1557109593000]
 * WinodwFuncation invoked, Window:[2019-05-06 10:26:30,2019-05-06 10:26:33]
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * (hqbhoho,400,1557109590000)
 * watermark generate, watermark: 1557109593000
 * WinodwFuncation invoked, Window:[1557109590000,1557109593000]
 * WinodwFuncation invoked, Window:[2019-05-06 10:26:30,2019-05-06 10:26:33]
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * (hqbhoho,600,1557109590000)
 * watermark generate, watermark: 1557109593000
 * WinodwFuncation invoked, Window:[1557109590000,1557109593000]
 * WinodwFuncation invoked, Window:[2019-05-06 10:26:30,2019-05-06 10:26:33]
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * (hqbhoho,800,1557109590000)
 * watermark generate, watermark: 1557109593000
 * watermark generate, watermark: 1557109594000
 * watermark generate, watermark: 1557109594000
 * watermark generate, watermark: 1557109594000
 * 可以看出，我们Window允许迟到1是,这样属于Window:[2019-05-06 10:26:30,2019-05-06 10:26:33]的元素，
 * 只要在watermark: 1557109594000生成以前到达就可以重新触发window操作。watermark: 1557109594000生成之后晚到的数据将会被丢弃
 * ****************************************************************************************
 * <p>
 * 测试场景 4 (process Lateness event)
 * nc -lk 19999
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109595000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109596000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * 结果：
 * watermark generate, watermark: 1557109590000
 * watermark generate, watermark: 1557109593000
 * WinodwFuncation invoked, Window:[1557109590000,1557109593000]
 * WinodwFuncation invoked, Window:[2019-05-06 10:26:30,2019-05-06 10:26:33]
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * (hqbhoho,400,1557109590000)
 * watermark generate, watermark: 1557109593000
 * WinodwFuncation invoked, Window:[1557109590000,1557109593000]
 * WinodwFuncation invoked, Window:[2019-05-06 10:26:30,2019-05-06 10:26:33]
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * (hqbhoho,600,1557109590000)
 * watermark generate, watermark: 1557109593000
 * WinodwFuncation invoked, Window:[1557109590000,1557109593000]
 * WinodwFuncation invoked, Window:[2019-05-06 10:26:30,2019-05-06 10:26:33]
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * (hqbhoho,800,1557109590000)
 * watermark generate, watermark: 1557109593000
 * watermark generate, watermark: 1557109594000
 * late-data: hqbhoho,200,1557109591000
 * watermark generate, watermark: 1557109594000
 * late-data: hqbhoho,200,1557109592000
 * watermark generate, watermark: 1557109594000
 * 可以将窗口迟到的数据收集到OutputTag中，在集中进行处理。
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/06
 */
public class EventTimeAndWatermarkExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 使用event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        // 创建数据源
        DataStreamSource<String> inputStream = env.socketTextStream("10.105.1.182", 19999);
        // 处理迟到的数据
        OutputTag<Tuple3<String, Integer, Long>> lateOutputTag = new OutputTag<Tuple3<String, Integer, Long>>("late-data") {
        };
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> results = inputStream.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String line) throws Exception {
                String[] list = line.split(",");
                return new Tuple3<String, Integer, Long>(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
            }
        }).assignTimestampsAndWatermarks(new MyTimeExtractor())
                .keyBy(t -> t.f0)
                .timeWindow(Time.seconds(3))
                // Window allowedLateness
                .allowedLateness(Time.seconds(1))
                // process late event
                .sideOutputLateData(lateOutputTag)
                // window funcation
                .apply(new WindowFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Optional.ofNullable("WinodwFuncation invoked, Window:[" + window.getStart() + "," + window.getEnd() + "]").ifPresent(System.out::println);
                        Optional.ofNullable("WinodwFuncation invoked, Window:[" + sdf.format(new Date(window.getStart())) + "," + sdf.format(new Date(window.getEnd())) + "]").ifPresent(System.out::println);
                        int count = 0;
                        Iterator<Tuple3<String, Integer, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple3<String, Integer, Long> next = it.next();
                            count += next.f1;
                            Optional.ofNullable(next.f0 + "," + next.f1 + "," + next.f2).ifPresent(System.out::println);
                        }
                        out.collect(new Tuple3<>(key, count, window.getStart()));
                    }
                });
        results.print();
        DataStream<Tuple3<String, Integer, Long>> lateDataStream = results.getSideOutput(lateOutputTag);
        lateDataStream.map(new MapFunction<Tuple3<String, Integer, Long>, String>() {
            @Override
            public String map(Tuple3<String, Integer, Long> t) throws Exception {
                return "late-data: " + t.f0 + "," + t.f1 + "," + t.f2;
            }
        }).print();
        env.execute("EventTimeAndWatermarkExample");
    }

    public static class MyTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>> {

        private final long maxOutOfOrderness = 2000; // 2 seconds

        private long currentMaxTimestamp = 0;

        @Override
        public Watermark getCurrentWatermark() {
            Optional.ofNullable("Thread: " + Thread.currentThread().getId() + ",watermark generate, watermark: " + (currentMaxTimestamp - maxOutOfOrderness)).ifPresent(System.out::println);
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3 tuple3, long l) {
            long timeStamp = (long) tuple3.f2;
            currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
            return timeStamp;
        }
    }
}
