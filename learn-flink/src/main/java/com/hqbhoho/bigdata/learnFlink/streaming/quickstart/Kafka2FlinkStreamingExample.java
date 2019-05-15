package com.hqbhoho.bigdata.learnFlink.streaming.quickstart;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * describe:
 * Flink consumer Kafka message
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/19
 */
public class Kafka2FlinkStreamingExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(
                java.util.regex.Pattern.compile("gx-test"),
                new SimpleStringSchema(),
                loadProp());


//        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>("",
//                new SimpleStringSchema(),
//                loadProp()
//                );


        DataStreamSource<String> source = env.addSource(myConsumer, "Kafka-source-1");

//        source.assignTimestampsAndWatermarks(new  );
        env.execute("Kafka Eaxmple");
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "nn01.hbzq.com:9092,nn02.hbzq.com:9092,ut01.hbzq.com:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "hqbhoho001");
        props.put("client.id", "hqbhoho-client");
        props.put("enable.auto.commit",false);
        props.put("auto.offset.reset","earliest");
        return props;
    }

}
