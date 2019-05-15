package com.hqbhoho.bigdata.learnFlink.streaming.stateAndCheckpoint;

import com.hqbhoho.bigdata.learnFlink.streaming.pojo.AccountOperatorStatistics;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * describe:
 * StateSchemaEvolution   Flink v1.8  is supported only for POJO and Avro types.
 * <p>
 * POJO type example
 * <p>
 * 1. 源POJO字段
 * Account
 * AccountOperatorCount
 * AccountMoneyCount
 * 2.运行程序，测试
 * nc -l 29999
 * hqbhoho,200
 * hb,20
 * hqbhoho,400
 * 3.状态及输出
 * AccountOperatorStatistics{Account='hqbhoho', AccountOperatorCount=1, AccountMoneyCount=200}
 * AccountOperatorStatistics{Account='hb', AccountOperatorCount=1, AccountMoneyCount=20}
 * AccountOperatorStatistics{Account='hqbhoho', AccountOperatorCount=2, AccountMoneyCount=600}
 * 4.修改POJO字段信息
 * 删除 AccountOperatorCount
 * 增加 AccountOperatorInCountn
 * 增加 AccountOperatorOutCount
 * 5.停止正在运行的程序
 * /opt/module/flink-1.8.0/bin/flink cancel -s file:///root/checkpoint 47240cc4bdb8ccd137712d33df9d6104（jobid）
 * 6.重新运行改好的程序
 * ./bin/flink run -s /root/checkpoint/savepoint-47240c-50d09e8c2516/_metadata -c com.hqbhoho.bigdata.learnFlink.streaming.stateAndCheckpoint.StateSchemaEvolutionExample /opt/learn-flink-1.0-SNAPSHOT.jar --host 192.168.5.131 --port 29999
 * 7.测试
 * nc -l 29999
 * hqbhoho,-300
 * hqbhoho,-100
 * hb,-400
 * hqbhoho,100
 * 8.状态及输出
 * AccountOperatorStatistics{Account='hqbhoho', AccountOperatorInCount=0, AccountOperatorOutCount=1, AccountMoneyCount=300}
 * AccountOperatorStatistics{Account='hqbhoho', AccountOperatorInCount=0, AccountOperatorOutCount=2, AccountMoneyCount=200}
 * AccountOperatorStatistics{Account='hb', AccountOperatorInCount=0, AccountOperatorOutCount=1, AccountMoneyCount=-380}
 * AccountOperatorStatistics{Account='hqbhoho', AccountOperatorInCount=1, AccountOperatorOutCount=2, AccountMoneyCount=300}
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/10
 */
public class StateSchemaEvolutionExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "192.168.5.131");
        String port = tool.get("port", "39999");
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置数据源
        env.socketTextStream(host, Integer.valueOf(port))
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) throws Exception {
                        String[] list = line.split(",");
                        return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
                    }
                })
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Integer>, AccountOperatorStatistics>() {

                    private ValueState<AccountOperatorStatistics> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<AccountOperatorStatistics> valueStateDescriptor = new ValueStateDescriptor<>(
                                "valueStateDescriptor",
                                AccountOperatorStatistics.class
                        );
                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public AccountOperatorStatistics map(Tuple2<String, Integer> value) throws Exception {
                        AccountOperatorStatistics oldCount = valueState.value();
                        if (oldCount == null) {
                            oldCount = AccountOperatorStatistics.of(value.f0, 0, 0, 0);
                        }
                        AccountOperatorStatistics newCount = null;
                        if (value.f1 > 0) {
                            newCount = AccountOperatorStatistics.of(value.f0, oldCount.getAccountOperatorInCount() + 1, oldCount.getAccountOperatorOutCount(), oldCount.getAccountMoneyCount() + value.f1);
                        } else {
                            newCount = AccountOperatorStatistics.of(value.f0, oldCount.getAccountOperatorInCount(), oldCount.getAccountOperatorOutCount() + 1, oldCount.getAccountMoneyCount() + value.f1);
                        }

                        valueState.update(newCount);
                        return newCount;
                    }
                }).uid("hqbhohostate001").print();
        env.execute("StateSchemaEvolutionExample");
    }
}
