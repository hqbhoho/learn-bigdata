package com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream;

import com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.EventTimeAndWatermarkExample;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.PeriodicWatermarkAssigner;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * describe:
 * Event time allows a table program to produce results based on the time that is contained in every record.
 * <p>
 * 1. During DataStream-to-Table Conversion
 * 2. Using a TableSource
 * you can
 * 1. appended as a new field to the schema as event time
 * 2. replaces an existing field as event time
 * <p>
 * Test
 * nc -l 19999
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109593000
 * hqbhoho,200,1557109595000
 * <p>
 * result
 * Thread: 56,watermark generate, watermark: 1557109591000
 * Thread: 56,watermark generate, watermark: 1557109593000
 * hqbhoho,800,2019-05-06 02:26:30.0,2019-05-06 02:26:33.0,2019-05-06 02:26:32.999
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/28
 */
public class EventTimeTableExample {
    public static void main(String[] args) throws Exception {
        // During DataStream-to-Table Conversion and appended as a new field to the schema as event time
//        DataStream2TableAddTimeFieldProcess(args);
        // Using a TableSource and replaces an existing field as event time
        TableSourceReuseTimeFieldProcess(args);


    }

    private static void TableSourceReuseTimeFieldProcess(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port1", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);

        tableEnv.registerTableSource("Register-Table-Source", new UserActionSource(host, port));

        Table resultTable = tableEnv.scan("Register-Table-Source")
                .window(Tumble.over("3.seconds").on("timeStamp").as("EventTimeWindow"))
                .groupBy("EventTimeWindow,name")
                .select("name,account.sum as sum,EventTimeWindow.start,EventTimeWindow.end,EventTimeWindow.rowtime");

        // Table ---> DataStream
        tableEnv.toAppendStream(
                resultTable,
                TypeInformation.of(new TypeHint<Row>() {
                }))
                .print();

        env.execute("EventTimeTableExample");
    }

    /**
     * define a table source with a processing attribute
     */
    public static class UserActionSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

        private String host;
        private int port;

        public UserActionSource(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {

            // 定义时间属性，以及watermark生成
            RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
                    // tableschema 属性
                    "timeStamp",
                    // datastream 字段
                    new ExistingField("timeStamp"),
//                    new StreamRecordTimestamp(),
                    new MyWaterMarkGenerate()
            );
            List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
            return listRowtimeAttrDescr;

        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
            return env.socketTextStream(host, port)
                    .map(new MapFunction<String, Row>() {
                        @Override
                        public Row map(String line) throws Exception {
                            String[] list = line.split(",");
                            return Row.of(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
                        }
                    })
                    .returns(getReturnType());
        }

        @Override
        public TypeInformation<Row> getReturnType() {
            String[] names = new String[]{"name", "account", "timeStamp"};
            TypeInformation[] types = new TypeInformation[]{
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO};
            return new RowTypeInfo(types, names);
        }

        @Override
        public TableSchema getTableSchema() {
            String[] names = new String[]{"name", "account", "timeStamp"};
            TypeInformation[] types = new TypeInformation[]{
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    TypeInformation.of(Timestamp.class)};
            return new TableSchema(names, types);
        }
    }

    private static class MyWaterMarkGenerate extends PeriodicWatermarkAssigner {

        private final Long maxOutOfOrderness = 2000L; // 2 seconds
        private Long currentMaxTimestamp = 0L;

        @Override
        public void nextTimestamp(long timestamp) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        }

        @Override
        public Watermark getWatermark() {
            Optional.ofNullable("Thread ID: " + Thread.currentThread().getId() + ",watermark generate, watermark: " + (currentMaxTimestamp - maxOutOfOrderness))
                    .ifPresent(System.out::println);
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

    }

    private static void DataStream2TableAddTimeFieldProcess(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port1", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);

        // 提取时间戳，生成watermark
        DataStream<Tuple3<String, Integer, Long>> input = env.socketTextStream(host, port)
                .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String line) throws Exception {
                        String[] list = line.split(",");
                        return new Tuple3<String, Integer, Long>(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
                    }
                }).assignTimestampsAndWatermarks(new EventTimeAndWatermarkExample.MyTimeExtractor());
        // DataStream ---> Table
        // appended as a new field to the schema as event time
        Table inputTable = tableEnv.fromDataStream(input, "name,account,timestamp,EventTime.rowtime");

        Table result = inputTable.window(Tumble.over("3.seconds").on("EventTime").as("EventTimeWindow"))
                .groupBy("EventTimeWindow,name")
                .select("name,account.sum as sum,EventTimeWindow.start,EventTimeWindow.end,EventTimeWindow.rowtime");

        // Table ---> DataStream
        tableEnv.toAppendStream(
                result,
                TypeInformation.of(new TypeHint<Row>() {
                }))
                .print();

        env.execute("EventTimeTableExample");
    }
}
