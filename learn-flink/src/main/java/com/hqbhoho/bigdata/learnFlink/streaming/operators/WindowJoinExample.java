package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

/**
 * describe:
 * <p>
 * Transformation Operator Example (DataStream,DataStream → DataStream)
 * window 触发条件是    两个流中较小的watermater > window_time_end
 * 测试：
 * 1. nc -l 29999
 * hqbhoho,1,1557109591000
 * hqbhoho,2,1557109592000
 * hqbhoho,3,1557109593000
 * hqbhoho,4,1557109594000
 * hqbhoho,5,1557109595000
 * hqbhoho,6,1557109596000
 * 2. nc -l 19999
 * hqbhoho,100,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,300,1557109593000(此时窗口触发)
 * 结果：
 * 58,watermater generate： 1557109596000
 * 59,watermater generate： 1557109592000
 * 58,watermater generate： 1557109596000
 * 59,watermater generate： 1557109593000
 * first timestamp：2019-05-06 10:26:31,second timestamp：2019-05-06 10:26:31
 * (hqbhoho,100,1)
 * first timestamp：2019-05-06 10:26:31,second timestamp：2019-05-06 10:26:32
 * (hqbhoho,100,2)
 * first timestamp：2019-05-06 10:26:32,second timestamp：2019-05-06 10:26:31
 * (hqbhoho,200,1)
 * first timestamp：2019-05-06 10:26:32,second timestamp：2019-05-06 10:26:32
 * (hqbhoho,200,2)
 * 59,watermater generate： 1557109593000
 * 58,watermater generate： 1557109596000
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/10
 */
public class WindowJoinExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        int port2 = tool.getInt("port2", 29999);
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置运行环境
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);
        // 获取数据
        DataStream<Tuple3<String, Integer, Long>> input1 = env.socketTextStream(host, port1)
                .map(new TokenFunction())
                .assignTimestampsAndWatermarks(new MyAssignTimestampsAndWatermarks());
        DataStream<Tuple3<String, Integer, Long>> input2 = env.socketTextStream(host, port2)
                .map(new TokenFunction())
                .assignTimestampsAndWatermarks(new MyAssignTimestampsAndWatermarks());

        input1.join(input2)
                .where(t -> t.f0).equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple3<String, Integer, Long> first, Tuple3<String, Integer, Long> second) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Optional.ofNullable("first timestamp：" + sdf.format(new Date(first.f2)) + ",second timestamp：" + sdf.format(new Date(second.f2))).ifPresent(System.out::println);
                        return Tuple3.of(first.f0, first.f1, second.f1);
                    }
                }).print();

        env.execute("WindowJoinExample");
    }

    /**
     * String -> Tuple3
     */
    static class TokenFunction implements MapFunction<String, Tuple3<String, Integer, Long>> {
        @Override
        public Tuple3<String, Integer, Long> map(String value) throws Exception {
            String[] list = value.split(",");
            return Tuple3.of(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
        }
    }

    /**
     * extract timestamp  and generate watermark
     */
    public static class MyAssignTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>> {

        private Long currentMaxTimeStamp = 0L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Optional.ofNullable(Thread.currentThread().getId() + ",watermater generate： " + currentMaxTimeStamp).ifPresent(System.out::println);
            return new Watermark(currentMaxTimeStamp);
        }

        @Override
        public long extractTimestamp(Tuple3<String, Integer, Long> element, long previousElementTimestamp) {

            Long timestamp = element.f2;
            currentMaxTimeStamp = Math.max(currentMaxTimeStamp, timestamp);
            return timestamp;
        }
    }


}
