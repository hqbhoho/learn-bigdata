package com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * describe:
 * Processing time allows a table program to produce results based on the time of the local machine.
 * <p>
 * 1. During DataStream-to-Table Conversion
 * 2. Using a TableSource
 * <p>
 * Exception in thread "main" org.apache.flink.table.api.ValidationException: Expression rowtime('userActionWindow) failed on input check: A proctime window cannot provide a rowtime attribute.
 * <p>
 * Test:
 * nc -l 19999
 * hqbhoho,200
 * hqbhoho,200
 * hqbhoho,200
 * hqbhoho,200
 * hqbhoho,200
 * hqbhoho,200
 * hqbhoho,200
 * hqbhoho,200
 * result
 * 4> (Thread ID: 62,convert table....,true,2019-05-28 07:20:45.0,2019-05-28 07:20:50.0,200)
 * 4> (Thread ID: 78,register table....,true,2019-05-28 07:20:40.0,2019-05-28 07:20:50.0,200)
 * 1> (Thread ID: 59,convert table....,true,2019-05-28 07:20:50.0,2019-05-28 07:20:55.0,400)
 * 1> (Thread ID: 64,register table....,true,2019-05-28 07:20:50.0,2019-05-28 07:21:00.0,800)
 * 2> (Thread ID: 60,convert table....,true,2019-05-28 07:20:55.0,2019-05-28 07:21:00.0,600)
 * 3> (Thread ID: 61,convert table....,true,2019-05-28 07:21:00.0,2019-05-28 07:21:05.0,400)
 * 2> (Thread ID: 65,register table....,true,2019-05-28 07:21:00.0,2019-05-28 07:21:10.0,600)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/28
 */
public class ProcessTimeTableExample {
    public static void main(String[] args) throws Exception {
        // 1. During DataStream-to-Table Conversion
//        DataStreamToTableConvertionProcess(args);
        // 2. Using a TableSource
        TableSourceProcess(args);
    }

    private static void TableSourceProcess(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port1", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.registerTableSource("Register-Table-Source-Example", new UserActionSource(host, port));

        Table result2 = tableEnv.scan("Register-Table-Source-Example")
                .window(
                        Tumble.over("10.seconds")
                                .on("UserActionTime")
                                .as("userActionWindow")
                )
                .groupBy("userActionWindow")
                .select("userActionWindow.start, userActionWindow.end,account.sum as sum");

        tableEnv.toAppendStream(result2, TypeInformation.of(new TypeHint<Row>() {
        }))
                .map(new MapFunction<Row, Tuple2<String, Row>>() {
                    @Override
                    public Tuple2<String, Row> map(Row value) throws Exception {
                        return Tuple2.of("Thread ID: " + Thread.currentThread().getId() + ",register table source....", value);
                    }
                })
                .print();
        env.execute("ProcessTimeTableExample");
    }

    /**
     * define a table source with a processing attribute
     */
    public static class UserActionSource implements StreamTableSource<Row>, DefinedProctimeAttribute {

        private String host;
        private int port;
        String[] names = new String[]{"name", "account"};
        TypeInformation[] types = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};

        public UserActionSource(String host, int port) {
            this.host = host;
            this.port = port;
        }


        @Override
        public TypeInformation<Row> getReturnType() {

            return new RowTypeInfo(types, names);
        }

        @Override
        public TableSchema getTableSchema() {
            String[] names = new String[]{"name", "account", "UserActionTime"};
            TypeInformation[] types = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Timestamp.class)};

            return new TableSchema(names, types);
        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
            // create stream
            DataStream<Row> input = execEnv.socketTextStream(host, port)
                    .map(new MapFunction<String, Row>() {
                        @Override
                        public Row map(String line) {
                            String[] list = line.split(",");
                            return Row.of(list[0], Integer.valueOf(list[1]));
                        }
                    }).returns(getReturnType());
            return input;
        }

        @Override
        public String getProctimeAttribute() {
            // field with this name will be appended as a third field
            return "UserActionTime";
        }
    }

    private static void DataStreamToTableConvertionProcess(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port1", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 模拟数据源
        DataStream<Tuple2<String, Integer>> input = env.socketTextStream(host, port)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) {
                        String[] list = line.split(",");
                        return new Tuple2<String, Integer>(list[0], Integer.valueOf(list[1]));
                    }
                });
        // datastream --> table
        Table table = tableEnv.fromDataStream(input, "name,account,timestamp.proctime");
        tableEnv.registerDataStream("Register-DataStream-Example", input, "name,account,timestamp.proctime");

        Table result1 = table.window(
                Tumble.over("5.seconds")
                        .on("timestamp")
                        .as("userActionWindow")
        )
                .groupBy("userActionWindow")
                .select("userActionWindow.start, userActionWindow.end,account.sum as sum");

        Table result2 = tableEnv.scan("Register-DataStream-Example").window(
                Tumble.over("10.seconds")
                        .on("timestamp")
                        .as("userActionWindow")
        )
                .groupBy("userActionWindow")
                .select("userActionWindow.start, userActionWindow.end,account.sum as sum");

        // table ---> dataStream
        tableEnv.toRetractStream(result1, TypeInformation.of(new TypeHint<Row>() {
        }))
                .map(new MapFunction<Tuple2<Boolean, Row>, Tuple3<String, Boolean, Row>>() {

                    @Override
                    public Tuple3<String, Boolean, Row> map(Tuple2<Boolean, Row> value) throws Exception {
                        return Tuple3.of("Thread ID: " + Thread.currentThread().getId() + ",convert table....", value.f0, value.f1);
                    }
                })
                .print();

        tableEnv.toRetractStream(result2, TypeInformation.of(new TypeHint<Row>() {
        }))
                .map(new MapFunction<Tuple2<Boolean, Row>, Tuple3<String, Boolean, Row>>() {

                    @Override
                    public Tuple3<String, Boolean, Row> map(Tuple2<Boolean, Row> value) throws Exception {
                        return Tuple3.of("Thread ID: " + Thread.currentThread().getId() + ",register table....", value.f0, value.f1);
                    }
                })
                .print();
        env.execute("ProcessTimeTableExample");
    }


}
