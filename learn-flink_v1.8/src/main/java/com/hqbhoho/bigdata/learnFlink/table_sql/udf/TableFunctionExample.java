package com.hqbhoho.bigdata.learnFlink.table_sql.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * 自定义UDF Table Function
 * In the Table API, a table function is used with .joinLateral or .leftOuterJoinLateral
 * <p>
 * eg: "hello world hello flinksql"
 * ---> ("hello world hello flinksql","hello",5),("hello world hello flinksql","world",5)
 * ---> ("hello world hello flinksql","hello",5),("hello world hello flinksql","flinksql",8)
 * <p>
 * result:
 * joinLateral output:
 * 3> hello world hello flinksql,hello,5
 * 3> hello world hello flinksql,world,5
 * 3> hello world hello flinksql,hello,5
 * 3> hello world hello flinksql,flinksql,8
 * <p>
 * leftOuterJoinLateral output:
 * 4> hello,null,null
 * 3> hello world hello flinksql,hello,5
 * 3> hello world hello flinksql,world,5
 * 3> hello world hello flinksql,hello,5
 * 3> hello world hello flinksql,flinksql,8
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/30
 */
public class TableFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册函数
        tableEnv.registerFunction("split", new split(" "));

        DataStreamSource<String> input = env.fromElements(
                "hello world hello flinksql",
                "hello"
        );

        tableEnv.registerDataStream("example", input, "str");

        // use the function in Java Table API
        Table table1 = tableEnv.scan("example")
                .joinLateral("split(str) as (value,length)")
                .select("str,value,length");
        Table table2 = tableEnv.scan("example")
                .leftOuterJoinLateral("split(str) as (value,length)")
                .select("str,value,length");
        // Use the table function in SQL with LATERAL and TABLE keywords.
        Table table3 = tableEnv.sqlQuery("select str,word,length from example,LATERAL TABLE(split(str)) as T(word, length)");
        Table table4 = tableEnv.sqlQuery("select str,word,length from example left join LATERAL TABLE(split(str)) as T(word, length) on true");

        // table ---> datastream
        tableEnv.toAppendStream(table1, Row.class).print();
        tableEnv.toAppendStream(table2, Row.class).print();
        tableEnv.toAppendStream(table3, Row.class).print();
        tableEnv.toAppendStream(table4, Row.class).print();

        env.execute("TableFunctionExample");
    }

    /**
     * hello world -> (hello,5),(world,5)
     */
    public static class split extends TableFunction<Tuple2<String, Integer>> {
        private String separator;

        public split() {
        }

        public split(String separator) {
            this.separator = separator;
        }

        public void eval(String s) {
            eval(s, separator);
        }

        public void eval(String s, String separator) {
            String[] split = s.split(separator);
            if (split.length == 1) return;
            for (String value : split) {
                collect(Tuple2.of(value, value.length()));
            }
        }
    }
}
