package com.hqbhoho.bigdata.learnFlink.batch.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;

import java.util.Optional;

/**
 * describe:
 * 全量迭代
 * <p>
 * 迭代结束条件：
 * 1. 达到最大迭代次数
 * 2. 自定义收敛条件
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/27
 */
public class IterateOperatorExample {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 模拟数据源
        DataSource<String> input = env.fromElements(
                "hqba001",
                "hqbaa002",
                "hqb",
                "hqbaaa003",
                "hqbaaaa004"
        );
        // 将a全部替换成@   这里我们定义的是最大的迭代次数10       也可以定义迭代收敛条件
        IterativeDataSet<String> initial = input.iterate(10);
        // 此方法会被调用10次
        MapOperator<String, String> it = initial.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                Optional.ofNullable("<<<>>>process event: " + value).ifPresent(System.out::println);
                if (value.contains("a")) value = value.replaceFirst("a", "@");
                return value;
            }
        });
        DataSet<String> result = initial.closeWith(it);
        result.map(v -> "result: " + v).print();
    }
}
