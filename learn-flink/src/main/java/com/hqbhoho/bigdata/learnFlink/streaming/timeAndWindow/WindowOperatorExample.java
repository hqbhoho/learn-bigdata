package com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

/**
 * describe:
 * <p>
 * tumbling windows
 * sliding windows
 * session windows
 * global windows
 * <p>
 * event time window trigger by watermark
 * <p>
 * reference:
 * http://wuchong.me/blog/2016/05/25/flink-internals-window-mechanism/
 * http://wuchong.me/blog/2016/06/06/flink-internals-session-window/
 * *************************************************************************************************************************
 * Session window    [timestamp, timestamp+sessionGap)
 * <p>
 * test1: Session Window
 * nc -l 19999
 * hqbhoho,100,1557109591000  // watermark generate, watermark: 1557109589000    event session window: [1557109591000,1557109594000)----> actal session window:[1557109591000,1557109594000)
 * hqbhoho,200,1557109592000  // watermark generate, watermark: 1557109590000    event session window: [1557109592000,1557109595000)--merge--> actal session window:[1557109591000,1557109595000)
 * hqbhoho,700,1557109597000  // watermark generate, watermark: 1557109595000    watermark trigger session window [1557109591000,1557109595000) compute
 * result:
 * Thread: 54,WinodwFuncation invoked, Window:[1557109591000,1557109595000],maxTimeStamp: 1557109594999
 * Thread: 54,WinodwFuncation invoked, Window:[2019-05-06 10:26:31,2019-05-06 10:26:35]
 * (hqbhoho,2,300)
 * <p>
 * test2: Session Window (event out-of-order and late)
 * nc -l 19999
 * hqbhoho,100,1557109591000 // watermark generate, watermark: 1557109589000    event session window: [1557109591000,1557109594000)----> actal session window:[1557109591000,1557109594000)
 * hqbhoho,200,1557109592000 // watermark generate, watermark: 1557109590000    event session window: [1557109592000,1557109595000)--merge--> actal session window:[1557109591000,1557109595000)
 * hqbhoho,150,1557109591500 // watermark generate, watermark: 1557109590000    event session window: [1557109591500,1557109594500)--merge--> actal session window:[1557109591000,1557109595000)
 * hqbhoho,110,1557109591100 // watermark generate, watermark: 1557109590000    event session window: [1557109591100,1557109594100)--merge--> actal session window:[1557109591000,1557109595000)
 * hqbhoho,700,1557109597000 // watermark generate, watermark: 1557109595000    watermark trigger session window [1557109591000,1557109595000) compute
 * hqbhoho,110,1557109591100 // watermark generate, watermark: 1557109595000    watermark trigger session window [1557109591000,1557109595000) compute
 * hqbhoho,150,1557109591500 // watermark generate, watermark: 1557109595000    watermark trigger session window [1557109591000,1557109595000) compute
 * hqbhoho,800,1557109598000 // watermark generate, watermark: 1557109596000
 * hqbhoho,110,1557109591100 // watermark generate, watermark: 1557109596000     this event is too late to discard
 * result:
 * Thread: 54,WinodwFuncation invoked, Window:[1557109591000,1557109595000],maxTimeStamp: 1557109594999
 * Thread: 54,WinodwFuncation invoked, Window:[2019-05-06 10:26:31,2019-05-06 10:26:35]
 * (hqbhoho,4,560)
 * <p>
 * Thread: 54,WinodwFuncation invoked, Window:[1557109591000,1557109595000],maxTimeStamp: 1557109594999
 * Thread: 54,WinodwFuncation invoked, Window:[2019-05-06 10:26:31,2019-05-06 10:26:35]
 * (hqbhoho,5,670)
 * <p>
 * Thread: 54,WinodwFuncation invoked, Window:[1557109591000,1557109595000],maxTimeStamp: 1557109594999
 * Thread: 54,WinodwFuncation invoked, Window:[2019-05-06 10:26:31,2019-05-06 10:26:35]
 * (hqbhoho,6,820)
 * <p>
 * test3: Session Window (Session merge and event out-of-order and late)
 * nc -l 19999
 * hqbhoho,100,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,800,1557109598000   Window:[1557109591000,1557109595000) will be delete
 * hqbhoho,300,1557109593000   will not merge [1557109591000,1557109595000)
 * <p>
 * Thread: 54,WinodwFuncation invoked, Window:[1557109591000,1557109595000],maxTimeStamp: 1557109594999
 * Thread: 54,WinodwFuncation invoked, Window:[2019-05-06 10:26:31,2019-05-06 10:26:35]
 * (hqbhoho,2,300)
 * <p>
 * Thread: 54,WinodwFuncation invoked, Window:[1557109593000,1557109596000],maxTimeStamp: 1557109595999
 * Thread: 54,WinodwFuncation invoked, Window:[2019-05-06 10:26:33,2019-05-06 10:26:36]
 * (hqbhoho,1,300)
 * <p>
 * nc -l
 * hqbhoho,100,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,700,1557109597000  watermark trigger session window [1557109591000,1557109595000) compute
 * hqbhoho,300,1557109593000  will merge [1557109591000,1557109595000) to [1557109591000,1557109596000)
 * hqbhoho,800,1557109598000  watermark trigger session window [1557109591000,1557109596000) compute
 * <p>
 * Thread: 53,WinodwFuncation invoked, Window:[1557109591000,1557109595000],maxTimeStamp: 1557109594999
 * Thread: 53,WinodwFuncation invoked, Window:[2019-05-06 10:26:31,2019-05-06 10:26:35]
 * (hqbhoho,2,300)
 * <p>
 * Thread: 53,WinodwFuncation invoked, Window:[1557109591000,1557109596000],maxTimeStamp: 1557109595999
 * Thread: 53,WinodwFuncation invoked, Window:[2019-05-06 10:26:31,2019-05-06 10:26:36]
 * (hqbhoho,3,600)
 * <p>
 * test4: Session Window (parallel invoke)
 * window will be trigger when all upstream's watermark >= window.end_time
 * nc -l 19999
 * hqbhoho,100,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,300,1557109593000
 * hqbhoho,400,1557109594000  Window:[1557109591000,1557109597000)
 * hqbhoho,100,1557109591000  Window:[1557109591000,1557109597000)
 * hqbhoho,900,1557109599000  Thread: 68,watermark generate, watermark: 1557109597000
 * hqbhoho,900,1557109599000  Thread: 67,watermark generate, watermark: 1557109597000
 * hqbhoho,900,1557109599000  Thread: 69,watermark generate, watermark: 1557109597000
 * hqbhoho,900,1557109599000  Thread: 70,watermark generate, watermark: 1557109597000   trigger Window:[1557109591000,1557109597000)
 * <p>
 * Thread: 59,WinodwFuncation invoked, Window:[1557109591000,1557109597000],maxTimeStamp: 1557109596999
 * Thread: 59,WinodwFuncation invoked, Window:[2019-05-06 10:26:31,2019-05-06 10:26:37]
 * 2> (hqbhoho,5,1100)
 ** *************************************************************************************************************************
 * Global window  （not be achieved  now）
 * This windowing scheme is only useful if you also specify a custom trigger.
 * Otherwise, no computation will be performed,
 * as the global window does not have a natural end at which we could process the aggregated elements.
 * Test:
 *
 *
 *
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/14
 */
public class WindowOperatorExample {
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
//        default parallel = vcores num
//        env.setParallelism(1);
//        seesion window event process
        KeyedStream<Tuple3<String, Integer, Long>, String> input = env.socketTextStream(host, port1)
                .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String line) {
                        String[] list = line.split(",");
                        return new Tuple3<String, Integer, Long>(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
                    }
                })
//                 watermark = maxtimestamp
//                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks())
//                 watermark = maxtimestamp -2
                .assignTimestampsAndWatermarks(new EventTimeAndWatermarkExample.MyTimeExtractor())
                .keyBy(t -> t.f0);
//         event time session window
        SessionWindowOperator(input);
//        event time Global window
//        GlobalWindowOperator(input);

        env.execute("WindowOperatorExample");
    }

    /**
     * global window operator  暂未实现，后续实现
     * @param input
     */
    private static void GlobalWindowOperator(KeyedStream<Tuple3<String, Integer, Long>, String> input) {
        input.window(GlobalWindows.create())
                .trigger(new MyTrigger());
    }

    /**
     *  session window operator
     * @param input
     */
    private static void SessionWindowOperator(KeyedStream<Tuple3<String, Integer, Long>, String> input) {
        input.window(EventTimeSessionWindows.withGap(Time.seconds(3)))
                // window funcation allow late event
                //.allowedLateness(Time.seconds(1))
                .apply(new WindowFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Optional.ofNullable("Thread: " + Thread.currentThread().getId() + ",WinodwFuncation invoked, Window:[" + window.getStart() + "," + window.getEnd() + "],maxTimeStamp: " + window.maxTimestamp()).ifPresent(System.out::println);
                        Optional.ofNullable("Thread: " + Thread.currentThread().getId() + ",WinodwFuncation invoked, Window:[" + sdf.format(new Date(window.getStart())) + "," + sdf.format(new Date(window.getEnd())) + "]").ifPresent(System.out::println);
                        int count = 0;
                        int sum = 0;
                        Iterator<Tuple3<String, Integer, Long>> it = input.iterator();
                        while (it.hasNext()) {
                            Tuple3<String, Integer, Long> next = it.next();
                            count += 1;
                            sum += next.f1;
                        }
                        out.collect(Tuple3.of(key, count, sum));
                    }
                })
                .print();
    }

    /**
     * 后续竖线实现
     */
    private static class MyTrigger extends Trigger<Tuple3<String, Integer, Long>, GlobalWindow> {

        @Override
        public TriggerResult onElement(Tuple3<String, Integer, Long> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

        }
    }
}
