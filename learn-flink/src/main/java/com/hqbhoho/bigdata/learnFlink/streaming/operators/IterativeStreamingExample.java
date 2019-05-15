package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/**
 * describe:
 * Flink 迭代流
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/29
 */
public class IterativeStreamingExample {
    public static void main(String[] args) throws Exception {

        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取一个迭代流
        IterativeStream<String> it = env.socketTextStream("10.105.1.182", 19999).iterate();
        DataStream<String> repalcea = it.map(line -> {
            String s = line;
            if (line.contains("a")) {
                s = line.replaceFirst("a", "@@@@");
                Optional.ofNullable("method invoke......").ifPresent(System.out::println);
            }
            return s;
        });
        // 迭代收敛条件   line中不包含"a"
        it.closeWith(repalcea.filter(line -> line.contains("a")).setParallelism(1));
        repalcea.filter(line -> !line.contains("a")).print();
        env.execute("Iterative Stream Example");
    }
}
