package com.hqbhoho.bigdata.learnFlink.streaming.asyncio;

import com.hqbhoho.bigdata.learnFlink.streaming.operators.WindowJoinExample;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.*;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 * <p>
 * Unordered Async IO Operator (event time)
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
 * <p>
 * Test:
 * nc -l 19999
 * hqbhoho,100,1557109591000
 * xiaomixiu,100,1557109591000
 * xiaohaibo,100,1557109591000
 * wait 30s until  55,watermater generate： 1557109591000
 * xiaomixiu,200,1557109592000
 * xiaohaibo,200,1557109592000
 * <p>
 * result:
 * 55,watermater generate： 0
 * (xiaomixiu,100,1557109591000,25,female,1557908909974)
 * (xiaohaibo,100,1557109591000,26,male,1557908914929)
 * 55,watermater generate： 1557109591000
 * 55,watermater generate： 1557109592000
 * (hqbhoho,100,1557109591000,24,male,1557908965244)
 * (xiaohaibo,200,1557109592000,26,male,1557908942827)
 * (xiaomixiu,200,1557109592000,25,female,1557908938262)
 * <p>
 * 同一个watermark之间的数据是无序的，不同watermak的数据是有序的，
 * 即  即使下一个watermark的异步IO先返回，也要等到上一个watermark中的所有异步IO都返回了 才能输出。
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/15
 */
public class UnorderedEventTimeAsyncIOExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置运行环境
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(30000);
        env.setParallelism(1);
        // 获取数据
        DataStream<Tuple3<String, Integer, Long>> input1 = env.socketTextStream(host, port1)
                .map(new WindowJoinExample.TokenFunction())
                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks());
        // 无序
        AsyncDataStream.unorderedWait(input1, new AsyncDBQueryInfoRequest(), 70, TimeUnit.SECONDS, 10)
                .print();
        env.execute("UnorderedPorcessTimeAsyncIOExample");
    }

    /**
     * query db to get user info
     */
    static class AsyncDBQueryInfoRequest extends RichAsyncFunction<Tuple3<String, Integer, Long>, Tuple6<String, Integer, Long, Integer, String, Long>> {

        @Override
        public void asyncInvoke(Tuple3<String, Integer, Long> input, ResultFuture<Tuple6<String, Integer, Long, Integer, String, Long>> resultFuture) throws Exception {
            CompletableFuture.supplyAsync(() -> {
                Tuple6<String, Integer, Long, Integer, String, Long> result = null;
                Connection conn = null;
                try {
                    if (input.f0.contains("hqbhoho")) {
                        //test order
                        TimeUnit.SECONDS.sleep(60);
                    }
                    // query db
                    Class.forName("com.mysql.jdbc.Driver");
                    conn = DriverManager.getConnection("jdbc:mysql://192.168.5.131:3306/test", "root", "123456");
                    PreparedStatement pstmt = conn.prepareStatement("select age,gender from info where name= '" + input.f0 + "';");
                    ResultSet rs = pstmt.executeQuery();
                    rs.next();
                    int age = rs.getInt("age");
                    String gender = rs.getString("gender");
                    result = Tuple6.of(input.f0, input.f1, input.f2, age, gender, System.currentTimeMillis());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
                return result;
            }).whenComplete((v, e) -> {
                if (e == null && v != null) {
                    resultFuture.complete(Collections.singleton(v));
                } else if (e != null) {
                    resultFuture.completeExceptionally(e);
                } else {
                    resultFuture.completeExceptionally(new Exception("query fail,no data return....."));
                }
            });
        }

        /**
         * handle request timeout
         *
         * @param input
         * @param resultFuture
         * @throws Exception
         */
        @Override
        public void timeout(Tuple3<String, Integer, Long> input, ResultFuture<Tuple6<String, Integer, Long, Integer, String, Long>> resultFuture) throws Exception {
            Optional.ofNullable("Event : " + input + ",query request time out......").ifPresent(System.out::println);
            super.timeout(input, resultFuture);
        }
    }
}
