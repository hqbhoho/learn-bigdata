package com.hqbhoho.bigdata.learnFlink.table_sql.concepts.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * describe:
 * convert a DataStream into a Table and vice versa
 * <p>
 * DataStram  --->  Table
 * 1.Register a DataStream as Table
 * 2.Convert a DataStream into a Table
 * <p>
 * Table   ---> DataStram
 * There are two modes to convert a Table into a DataStream:
 * Append Mode: This mode can only be used if the dynamic Table is only modified by INSERT changes, i.e, it is append-only and previously emitted results are never updated.
 * Retract Mode: This mode can always be used. It encodes INSERT and DELETE changes with a boolean flag.
 * <p>
 * Test:
 * nc -l 19999
 * hqbhoho,100,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109592000
 * xioxio,300,1557109594000
 * hqbhoho,200,1557109592000
 * xioxio,300,1557109594000
 * result:
 * 1> Thread ID: 54,Append Mode,hqbhoho,100
 * 2> (Thread ID: 59,Retract Mode,true,hqbhoho,100)
 * 2> Thread ID: 55,Append Mode,hqbhoho,200
 * 2> (Thread ID: 59,Retract Mode,false,hqbhoho,100)
 * 2> (Thread ID: 59,Retract Mode,true,hqbhoho,300)
 * 3> Thread ID: 56,Append Mode,hqbhoho,200
 * 2> (Thread ID: 59,Retract Mode,false,hqbhoho,300)
 * 2> (Thread ID: 59,Retract Mode,true,hqbhoho,500)
 * 4> Thread ID: 57,Append Mode,xioxio,300
 * 3> (Thread ID: 60,Retract Mode,true,xioxio,300)
 * 1> Thread ID: 54,Append Mode,hqbhoho,200
 * 2> (Thread ID: 59,Retract Mode,false,hqbhoho,500)
 * 2> (Thread ID: 59,Retract Mode,true,hqbhoho,700)
 * 2> Thread ID: 55,Append Mode,xioxio,300
 * 3> (Thread ID: 60,Retract Mode,false,xioxio,300)
 * 3> (Thread ID: 60,Retract Mode,true,xioxio,600)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/27
 */
public class DataStreamAndTableConvertExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port = tool.getInt("port1", 19999);
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 模拟数据源
        DataStream<Tuple3<String, Integer, Long>> input = env.socketTextStream(host, port)
                .map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public Tuple3<String, Integer, Long> map(String line) {
                        String[] list = line.split(",");
                        return new Tuple3<String, Integer, Long>(list[0], Integer.valueOf(list[1]), Long.valueOf(list[2]));
                    }
                });
        /*DataStram  --->  Table*/
        // Register a DataStream as Table
        tableEnv.registerDataStream("Register-DataStream-example", input, "name,account,timestamp");
        // Convert a DataStream into a Table
        Table inputTable = tableEnv.fromDataStream(input, "name,account,timestamp");

        // data process
        Table result1 = tableEnv.scan("Register-DataStream-example")
                .select("name,account");

        Table result2 = inputTable.groupBy("name")
                .select("name,account.sum as sum");

        /* DataStram  --->  Table */
        TypeInformation<Row> type = TypeInformation.of(new TypeHint<Row>() {
        });
        //  Append Mode      append only stream
        tableEnv.toAppendStream(result1, type)
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) throws Exception {
                        return Row.of("Thread ID: " + Thread.currentThread().getId() + ",Append Mode", value.getField(0), value.getField(1));
                    }
                })
                .print();
        //  Retract Mode
        tableEnv.toRetractStream(result2, type)
                .map(new MapFunction<Tuple2<Boolean, Row>, Tuple3<String, Boolean, Row>>() {
                    @Override
                    public Tuple3<String, Boolean, Row> map(Tuple2<Boolean, Row> value) throws Exception {
                        return Tuple3.of("Thread ID: " + Thread.currentThread().getId() + ",Retract Mode", value.f0, value.f1);
                    }
                })
                .print();
        ;

        // run project
        env.execute("DataStreamAndTableConvertExample");
    }
}
