package com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow;

import com.hqbhoho.bigdata.learnFlink.streaming.operators.WindowJoinExample;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * describe:
 * Incremental Window Aggregation with AggregateFunction
 * Test:
 * nc -l 19999
 * hqbhoho,100,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,300,1557109593000  trigger window [1557109590000,1557109593000)
 * <p>
 * (hqbhoho,150.0,1557109590000,1557109593000)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/14
 */
public class ProcessWindowFunctionExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "192.168.5.131");
        int port1 = tool.getInt("port1", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        // 设置并行度
        env.setParallelism(1);
        // 数据处理
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
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                // Incremental Window Aggregation with AggregateFunction
                .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction())
                .print();
        env.execute("ProcessWindowFunctionExample");
    }

    /**
     * The accumulator is used to keep a running sum and a count. The {@code getResult} method
     * computes the average.
     * (key,value,timestamp) -> average
     */
    private static class MyAggregateFunction implements AggregateFunction<Tuple3<String, Integer, Long>, Tuple2<Integer, Integer>, Double> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Tuple3<String, Integer, Long> value, Tuple2<Integer, Integer> accumulator) {
            return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value.f1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return ((double) accumulator.f1 / accumulator.f0);
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    /**
     * (average) -> (key,average,window.start_time,window.end_time)
     */
    private static class MyProcessWindowFunction extends ProcessWindowFunction<Double, Tuple4<String, Double, Long, Long>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Double> elements, Collector<Tuple4<String, Double, Long, Long>> out) throws Exception {
            Double next = elements.iterator().next();
            // 获取窗口的元数据
            TimeWindow window = context.window();
            out.collect(Tuple4.of(key, next, window.getStart(), window.getEnd()));
        }
    }
}
