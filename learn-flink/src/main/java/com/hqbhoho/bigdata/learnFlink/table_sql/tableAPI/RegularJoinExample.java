package com.hqbhoho.bigdata.learnFlink.table_sql.tableAPI;

import com.hqbhoho.bigdata.learnFlink.streaming.operators.WindowJoinExample;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * 基于DATAStream RegularJoin为例
 * Exception in thread "main" org.apache.flink.table.api.ValidationException: join relations with ambiguous names: name, time
 * Join两边的字段名称不要一样
 * Test： InnerJoin
 * nc -l 19999
 * hqbhoho01,100,1557109591000    1     output: nothing
 * hqbhoho01,101,1557109591000    2     output: nothing
 * hqbhoho02,200,1557109592000    5     output: nothing
 * hqbhoho01,102,1557109591000    6     output: (true,hqbhoho01,102,11),(true,hqbhoho01,102,10)
 * nc -l 19999
 * hqbhoho01,10,1557109591000     3     output:  (true,hqbhoho01,100,10),(true,hqbhoho01,101,10)
 * hqbhoho01,11,1557109591000     4     output:  (true,hqbhoho01,100,11),(true,hqbhoho01,101,11)
 * hqbhoho02,20,1557109592000     7     output:  (true,hqbhoho02,200,20)
 * hqbhoho01,12,1557109591000     8     output:  (true,hqbhoho01,100,12),(true,hqbhoho01,101,12),(true,hqbhoho01,102,12)
 * <p>
 * Test: OutterJoin
 * 基于DataStream 的 OutterJoin   表现同上
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/30
 */
public class RegularJoinExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "10.105.1.182");
        int port1 = tool.getInt("port1", 19999);
        int port2 = tool.getInt("port2", 29999);
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取数据源
        DataStream<Tuple3<String, Integer, Long>> input1 = env.socketTextStream(host, port1)
                .map(new WindowJoinExample.TokenFunction());
//                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks());
        DataStream<Tuple3<String, Integer, Long>> input2 = env.socketTextStream(host, port2)
                .map(new WindowJoinExample.TokenFunction());
//                .assignTimestampsAndWatermarks(new WindowJoinExample.MyAssignTimestampsAndWatermarks());

        Table table1 = tableEnv.fromDataStream(input1, "name1,account,time1");
        Table table2 = tableEnv.fromDataStream(input2, "name2,num,time2");

        // inner join
        /*Table resultTable = table1.join(table2)
                .where("name1 = name2")
                .select("name1,account,num");*/
        Table resultTable = table1.leftOuterJoin(table2)
                .where("name1 = name2")
                .select("name1,account,num");

        tableEnv.toRetractStream(resultTable, Row.class)
                .print();
        env.execute("RegularJoinExample");
    }
}
