package com.hqbhoho.bigdata.learnFlink.streaming.asyncio;

import com.hqbhoho.bigdata.learnFlink.streaming.stateAndCheckpoint.BroadcastStateExample;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * describe:
 * <p>
 * Unordered Async IO Operator (process time)
 * <p>
 * ******************************************************************
 * Mysql table --> test.info
 * +----+-----------+------+--------+
 * | id | name      | age  | gender |
 * +----+-----------+------+--------+
 * |  1 | hqbhoho   |   24 | male   |
 * |  2 | xiaomixiu |   25 | female |
 * |  3 | xiaohaibo |   26 | male   |
 * +----+-----------+------+--------+
 * Test 1:
 * nc -l 19999
 * xiaomixiu,200
 * hqbhoho,400
 * xiaohaibo,500
 * xiaomixiu,200
 * results:
 * (xiaomixiu,200,25,female,1557906877522)
 * (xiaohaibo,500,26,male,1557906886108)
 * (hqbhoho,400,24,male,1557906890588)
 * (xiaomixiu,200,25,female,1557906891587)
 * 可以看出异步查询先返回先被输出，但是输出的顺序不同于输入的顺序，证明是无序的。
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/15
 */
public class UnorderedPorcessTimeAsyncIOExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        //获取数据源
        DataStream<Tuple2<String, String>> eventStream = env.socketTextStream(host, port)
                .map(new BroadcastStateExample.String2Tuple2MapperFuncation(","));
        // 无序
        AsyncDataStream.unorderedWait(eventStream, new OrderedAsyncIOExample.AsyncDBQueryInfoRequest(), 20, TimeUnit.SECONDS, 10)
                .print();
        env.execute("UnorderedPorcessTimeAsyncIOExample");
    }
}
