package com.hqbhoho.bigdata.learnFlink.batch.optimization;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * describe:
 * <p>
 * Via Constructor   通过operator处理类的构造函数
 * Via withParameters(Configuration)  operator.withParameters(Configuration)
 * Globally via the ExecutionConfig   全局参数配置
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/23
 */
public class ParametersToFunctionExample {
    public static void main(String[] args) throws Exception {
        // 模拟环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Integer> input = env.fromElements(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        // 配置参数
        Configuration conf = new Configuration();
        conf.setInteger("limit", 5);
       // Via withParameters(Configuration)
        /*
        // 模拟数据源
        input.filter(new RichFilterFunction<Integer>() {
            private Integer limit;

            @Override
            public void open(Configuration parameters) throws Exception {
                limit = parameters.getInteger("limit", 0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value >= limit;
            }
        }).withParameters(conf)
                .print();*/

        // Globally via the ExecutionConfig
        env.getConfig().setGlobalJobParameters(conf);
        input.filter(new RichFilterFunction<Integer>() {
            private Integer limit;
            @Override
            public void open(Configuration parameters) throws Exception {
                Configuration conf = (Configuration)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                limit = conf.getInteger("limit",0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value >= limit;
            }
        }).print();
    }
}
