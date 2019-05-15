package com.hqbhoho.bigdata.learnFlink.streaming.asyncio;

import com.hqbhoho.bigdata.learnFlink.streaming.stateAndCheckpoint.BroadcastStateExample;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
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
 * Ordered Async IO Operator (process time)
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
 * result:
 * (xiaomixiu,200,25,female,1557906226431)
 * (hqbhoho,400,24,male,1557906241183)
 * (xiaohaibo,500,26,male,1557906236417)
 * (xiaomixiu,200,25,female,1557906241999)
 * 可以看出虽然 xiaohaibo 异步查询先返回，但是输出的顺序还是按照输入的循序，证明是有序的。
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/15
 */
public class OrderedAsyncIOExample {
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
        // 有序
        AsyncDataStream.orderedWait(eventStream, new AsyncDBQueryInfoRequest(), 20, TimeUnit.SECONDS, 10)
                .print();
        env.execute("OrderedAsyncIOExample");
    }

    /**
     * query db to get user info
     */
    static class AsyncDBQueryInfoRequest extends RichAsyncFunction<Tuple2<String, String>, Tuple5<String, String, Integer, String, Long>> {

        @Override
        public void asyncInvoke(Tuple2<String, String> input, ResultFuture<Tuple5<String, String, Integer, String, Long>> resultFuture) throws Exception {
            CompletableFuture.supplyAsync(() -> {
                Tuple5<String, String, Integer, String, Long> result = null;
                Connection conn = null;
                try {
                    if (input.f0.contains("hqbhoho")) {
                        //test order
                        TimeUnit.SECONDS.sleep(10);
                    }
                    // query db
                    Class.forName("com.mysql.jdbc.Driver");
                    conn = DriverManager.getConnection("jdbc:mysql://192.168.5.131:3306/test", "root", "123456");
                    PreparedStatement pstmt = conn.prepareStatement("select age,gender from info where name= '" + input.f0 + "';");
                    ResultSet rs = pstmt.executeQuery();
                    rs.next();
                    int age = rs.getInt("age");
                    String gender = rs.getString("gender");
                    result = Tuple5.of(input.f0, input.f1, age, gender, System.currentTimeMillis());
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
        public void timeout(Tuple2<String, String> input, ResultFuture<Tuple5<String, String, Integer, String, Long>> resultFuture) throws Exception {
            Optional.ofNullable("Event : " + input + ",query request time out......").ifPresent(System.out::println);
            super.timeout(input, resultFuture);
        }
    }

}
