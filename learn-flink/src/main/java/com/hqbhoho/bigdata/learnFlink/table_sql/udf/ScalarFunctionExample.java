package com.hqbhoho.bigdata.learnFlink.table_sql.udf;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * describe:
 *
 * 自定义UDF   Scalar Function
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/30
 */
public class ScalarFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册函数
        tableEnv.registerFunction("regexp_like_1", new RegexpLike());
        tableEnv.registerFunction("regexp_like_2", new RegexpLike(".*xi.*"));

        DataStreamSource<Tuple4<String, Integer, String, Integer>> input = env.fromElements(
                Tuple4.of("hqbhoho", 21, "male", 10000),
                Tuple4.of("xiaomixiu", 22, "male", 20000),
                Tuple4.of("hqbhoho", 23, "male", 30000),
                Tuple4.of("xiaohaibo", 24, "male", 40000)
        );

        tableEnv.registerDataStream("example", input, "name,age,sex,account");

        // use the function in Java Table API
        Table result1 = tableEnv.scan("example")
                .where("name.regexp_like_2() = 1")
                .select("*");
        // use the function in SQL API
        Table result2 = tableEnv.sqlQuery("select * from example where regexp_like_1(name,'.*ho.*') = 0");

        tableEnv.toAppendStream(result1, Row.class).print();
        tableEnv.toAppendStream(result2, Row.class).print();

        env.execute("ScalarFunctionExample");

    }


    /**
     * 定义一个正则匹配的自定义函数  匹配到返回1   没匹配到返回 0
     */
    public static class RegexpLike extends ScalarFunction {
        private String regexp;

        public RegexpLike() {
        }

        public RegexpLike(String regexp) {
            this.regexp = regexp;
        }

        public int eval(String s) {
            return eval(s, regexp);
        }

        public int eval(String s, String regexp) {
            Pattern pattern = Pattern.compile(regexp);
            Matcher matcher = pattern.matcher(s);
            return matcher.matches() ? 1 : 0;
        }

        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return BasicTypeInfo.INT_TYPE_INFO;
        }
    }
}
