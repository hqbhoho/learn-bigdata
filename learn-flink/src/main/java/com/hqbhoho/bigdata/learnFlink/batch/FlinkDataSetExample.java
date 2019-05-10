package com.hqbhoho.bigdata.learnFlink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/23
 */
public class FlinkDataSetExample {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("/path/to/file");

        MapOperator<String, Integer> map = text.map(Integer::valueOf);

    }
}
