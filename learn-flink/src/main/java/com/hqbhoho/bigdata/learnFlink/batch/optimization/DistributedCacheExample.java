package com.hqbhoho.bigdata.learnFlink.batch.optimization;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * describe:
 * <p>
 * 分布式缓存，程序执行时，会copy一份到自己的local fileSystem
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/23
 */
public class DistributedCacheExample {
    public static void main(String[] args) throws Exception {
        //创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("file:///E:/javaProjects/learn-bigdata/learn-flink/src/main/resources/CountriesMap.csv", "countries_map");
        //模拟数据
        // 模拟数据源
        DataSource<Tuple4<String, String, Integer, String>> info = env.fromElements(
                Tuple4.of("CN", "hqbhoho001", 21, "male"),
                Tuple4.of("USA", "hqbhoho002", 22, "female"),
                Tuple4.of("ES", "hqbhoho003", 23, "female"),
                Tuple4.of("UN", "hqbhoho004", 24, "male")
        );
        info.map(new RichMapFunction<Tuple4<String, String, Integer, String>, Tuple4<String, String, Integer, String>>() {
            private Map<String, String> countries = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                BufferedReader br = null;
                try {
                    // 获取缓存文件
                    File countries_map = getRuntimeContext().getDistributedCache().getFile("countries_map");
                    br = new BufferedReader(new FileReader(countries_map));
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] values = line.split(",");
                        countries.put(values[1], values[0]);
                    }
                } finally {
                    if (br != null) {
                        br.close();
                    }
                }

            }

            @Override
            public Tuple4<String, String, Integer, String> map(Tuple4<String, String, Integer, String> value) throws Exception {
                return Tuple4.of(countries.get(value.f0), value.f1, value.f2, value.f3);
            }
        }).print();
    }
}
