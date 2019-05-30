package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * describe:
 * <p>
 * 类似sql中的count(distinct a)
 * Distinct can be applied to GroupBy Aggregation, GroupBy Window Aggregation and Over Window Aggregation.
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/30
 */
public class DistinctAggregationExample {
    public static void main(String[] args) {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


    }
}
