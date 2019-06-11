package com.hqbhoho.bigdata.learnFlink.table_sql.connectors.customer;

import com.hqbhoho.bigdata.learnFlink.streaming.timeAndWindow.EventTimeAndWatermarkExample;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * RetractStreamTableSink ---> 将结果直接输出到控制台，类似print的功能
 * Test:
 * nc -l 19999
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * <p>
 * results:
 * (true,hqbhoho,200)
 * (false,hqbhoho,200)
 * (true,hqbhoho,400)
 * (false,hqbhoho,400)
 * (true,hqbhoho,600)
 * (false,hqbhoho,600)
 * (true,hqbhoho,800)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/11
 */
public class UserDefinedSinksExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port1", 19999);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10000000);

        // 提取时间戳，生成watermark
        DataStream<Tuple3<String, Integer, Long>> input = env.socketTextStream(host, port)
                .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String line) throws Exception {
                        String[] list = line.split(",");
                        return new Tuple3<String, Integer, Long>(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
                    }
                }).assignTimestampsAndWatermarks(new EventTimeAndWatermarkExample.MyTimeExtractor());

        Table inputTable = tableEnv.fromDataStream(input, "name,account,timestamp,eventtime.rowtime");

        Table resultTable = tableEnv.sqlQuery("select name,sum(account) from " + inputTable + " group by name");

        //   注册user defined sink
        tableEnv.registerTableSink(
                "UserDefinedSink",
                new RetractStreamPrintTableSink
                        .Builder()
                        .fieldNames(new String[]{"name", "account"})
                        .fieldTypes(new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO})
                        .build()
        );
        tableEnv.sqlUpdate("insert into UserDefinedSink select * from " + resultTable);
        env.execute("UserDefinedSinksExample");
    }

    /**
     * User Define RetractStreamTableSink
     */
    public static class RetractStreamPrintTableSink implements RetractStreamTableSink<Row> {

        private String[] fieldNames;
        private TypeInformation<?>[] fieldTypes;

        private RetractStreamPrintTableSink(Builder builder) {
            this.fieldNames = builder.fieldNames;
            this.fieldTypes = builder.fieldTypes;
        }

        private RetractStreamPrintTableSink(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            this.fieldNames = fieldNames;
            this.fieldTypes = fieldTypes;
        }

        public static class Builder {
            private String[] fieldNames;
            private TypeInformation<?>[] fieldTypes;

            public Builder fieldNames(String[] fieldNames) {
                this.fieldNames = fieldNames;
                return this;
            }

            public Builder fieldTypes(TypeInformation<?>[] fieldTypes) {
                this.fieldTypes = fieldTypes;
                return this;
            }

            public RetractStreamPrintTableSink build() {
                return new RetractStreamPrintTableSink(this);
            }
        }

        @Override
        public TypeInformation<Row> getRecordType() {
            return TypeInformation.of(Row.class);
        }

        /**
         * 具体的sink操作，最终转化成了DataStream操作
         *
         * @param dataStream
         */
        @Override
        public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
            dataStream.print();
        }

        @Override
        public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
            return new TupleTypeInfo(BasicTypeInfo.BOOLEAN_TYPE_INFO, TypeInformation.of(Row.class));
        }

        @Override
        public String[] getFieldNames() {
            return this.fieldNames;
        }

        @Override
        public TypeInformation<?>[] getFieldTypes() {
            return this.fieldTypes;
        }

        @Override
        public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            return new RetractStreamPrintTableSink(fieldNames, fieldTypes);
        }
    }
}
