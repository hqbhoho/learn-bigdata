package com.hqbhoho.bigdata.learnFlink.streaming.stateAndCheckpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * describe:
 * Queryable State Example
 * <p>
 * flink-conf.yaml  queryable-state.enable: true   开启查询状态功能
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/09
 */
public class QueryableStateExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "192.168.5.131");
        String port = tool.get("port", "19999");
//        Map<String,String> map = new HashMap<>();
//        map.put("queryable-state.enable","true");

        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(map));

        DataStream<String> input = env.socketTextStream(host, Integer.valueOf(port));
        input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] list = line.split(",");
                return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
            }
        }).keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>>() {

                    private transient ListState<Tuple2<Integer, Integer>> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 创建一个状态描述
                        ListStateDescriptor<Tuple2<Integer, Integer>> countStateDescriptor =
                                new ListStateDescriptor<Tuple2<Integer, Integer>>(
                                        "countStateDescriptor",
                                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                                        })
                                );
                        countStateDescriptor.setQueryable("count-query");

                        // 获取一个状态
                        countState = getRuntimeContext().getListState(countStateDescriptor);

                    }

                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<String, Integer> input) throws Exception {
                        Iterable<Tuple2<Integer, Integer>> it = countState.get();
                        Tuple2<Integer, Integer> oldCount = Tuple2.of(0, 0);
                        if (it.iterator().hasNext()) {
                            oldCount = it.iterator().next();
                        }
                        Tuple2<Integer, Integer> newCount = Tuple2.of(oldCount.f0 + 1, oldCount.f1 + input.f1);
                        countState.update(Arrays.asList(newCount));
                        return newCount;
                    }
                }).print();

        env.execute("QueryableStateExample");
    }
}
