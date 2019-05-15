package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * describe:
 * Project Example
 * result:
 * 2> (hqbhoho,1)
 * 3> (hqbhoho,2)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/13
 */
public class ProjectExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Long>> input = env.fromElements(
                Tuple3.of("hqbhoho", 1, 1557109591000L), Tuple3.of("hqbhoho", 2, 1557109592000L)
        );
        input.project(0,1).print();
        env.execute("ProjectExample");
    }
}
