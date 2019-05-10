package com.hqbhoho.bigdata.learnFlink.streaming;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * describe:
 * DataSource Example
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/29
 */
public class DataSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = env.readFile(new TextInputFormat(new Path("E:\\test.txt")),
                "E:\\test.txt",
                FileProcessingMode.PROCESS_ONCE,
                1000L,
                TypeInformation.of(String.class));
        input.print();
        env.execute("DataSource Example");
    }



}
