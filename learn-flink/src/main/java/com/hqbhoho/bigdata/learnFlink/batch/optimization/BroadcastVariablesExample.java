package com.hqbhoho.bigdata.learnFlink.batch.optimization;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * describe:
 * <p>
 * 广播变量
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/23
 */
public class BroadcastVariablesExample {
    public static void main(String[] args) throws Exception {
        // 构建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 模拟数据源
        DataSource<Tuple4<String, String, Integer, String>> info = env.fromElements(
                Tuple4.of("1001", "hqbhoho001", 21, "male"),
                Tuple4.of("1002", "hqbhoho002", 22, "female")
        );
        DataSource<Tuple3<String, String, Integer>> order = env.fromElements(
                Tuple3.of("1001", "iphone", 2),
                Tuple3.of("1001", "computer", 1),
                Tuple3.of("1002", "mac", 4)
        );
        order.map(new RichMapFunction<Tuple3<String, String, Integer>, Tuple5<String, String, Integer, String, Integer>>() {

            private Map<String, Tuple4<String, String, Integer, String>> info_map = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取广播变量
                List<Tuple4<String, String, Integer, String>> info =
                        getRuntimeContext().getBroadcastVariable("consumer_info");
                for (Tuple4<String, String, Integer, String> value : info) {
                    info_map.put(value.f0, value);
                }
            }

            @Override
            public Tuple5<String, String, Integer, String, Integer> map(Tuple3<String, String, Integer> value) throws Exception {
                Tuple4<String, String, Integer, String> info = info_map.get(value.f0);
                return Tuple5.of(value.f0, info.f1, info.f2, value.f1, value.f2);
            }
        }).withBroadcastSet(info, "consumer_info")
                .print();
    }
}
