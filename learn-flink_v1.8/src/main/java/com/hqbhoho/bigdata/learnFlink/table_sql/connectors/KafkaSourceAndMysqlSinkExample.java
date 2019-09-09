package com.hqbhoho.bigdata.learnFlink.table_sql.connectors;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;

import java.util.Properties;

/**
 * describe:
 * <p>
 * Kakfa json message  --------->  Mysql
 * <p>
 * Kafka Producer:{@link com.hqbhoho.bigdata.learnKafka.producer.FlinkSerializerProducer}
 * mysql result:
 * +---------+---------+
 * | name    | account |
 * +---------+---------+
 * | item202 |   211.9 |
 * | item300 |   309.9 |
 * | item301 |   310.9 |
 * | item302 |   311.9 |
 * | item303 |   312.9 |
 * | item304 |   313.9 |
 * | item305 |   314.9 |
 * | item306 |   315.9 |
 * | item307 |   316.9 |
 * | item308 |   317.9 |
 * | item309 |   318.9 |
 * | item310 |   319.9 |
 * | item311 |   320.9 |
 * | item312 |   321.9 |
 * | item313 |   322.9 |
 * | item314 |   323.9 |
 * | item315 |   324.9 |
 * | item316 |   325.9 |
 * | item317 |   326.9 |
 * | item318 |   327.9 |
 * | item319 |   328.9 |
 * | item320 |   329.9 |
 * +---------+---------+
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/10
 */
public class KafkaSourceAndMysqlSinkExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
//        ParameterTool tool = ParameterTool.fromArgs(args);
//        String host = tool.get("host", "10.105.1.182");
//        int port1 = tool.getInt("port1", 19999);
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);


        // 创建Kafka Source
        tableEnv.connect(
                new Kafka()
                        .version("0.11")
                        .topic("flink-test-1")
                        .properties(loadProp())
                        .startFromGroupOffsets()
        )
                .withFormat(
                        new Json().deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("name", BasicTypeInfo.STRING_TYPE_INFO)
                                .field("account", BasicTypeInfo.DOUBLE_TYPE_INFO)
                                .field("rowtime1", Types.SQL_TIMESTAMP())
                                .rowtime(
                                        new Rowtime().timestampsFromField("eventTime")
                                                .watermarksPeriodicBounded(2000)
                                )
                )
                .inAppendMode()
                .registerTableSource("kafka_input");

        Table resultTable = tableEnv.sqlQuery("select " +
                "name," +
                "sum(account)" +
                "from kafka_input" +
                " group by TUMBLE(rowtime1, INTERVAL '3' SECOND),name");

        CsvTableSink sink1 = new CsvTableSink(
                "E:\\javaProjects\\learn-bigdata\\learn-flink\\src\\main\\resources\\sink.csv",                  // output path
                ",",                   // optional: delimit files by '|'
                1,                     // optional: write to a single file
                FileSystem.WriteMode.OVERWRITE);  // optional: override existing files

        tableEnv.registerTableSink(
                "csvOutputTable",
                // specify table schema
                new String[]{"f0", "f1"},
                new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO},
                sink1);

        // mysql sink
        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setBatchSize(1)
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://192.168.5.131:3306/test")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("insert into account1(name,account) values (?,?)")
                .setParameterTypes(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO)
                .build();
        tableEnv.registerTableSink(
                "output",
                // specify table schema
                new String[]{"name_1", "account_1"},
                new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO},
                sink);

        // Table API
//        resultTable.insertInto("output");
        tableEnv.sqlUpdate("insert into output select name,account from kafka_input");
        env.execute("KafkaSourceAndMysqlSinkExample");
    }

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092");
        props.put("group.id", "hqbhoho0002");
        props.put("enable.auto.commit", "false");
        return props;
    }
}
