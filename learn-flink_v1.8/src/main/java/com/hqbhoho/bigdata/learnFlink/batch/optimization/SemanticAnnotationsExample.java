package com.hqbhoho.bigdata.learnFlink.batch.optimization;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * describe:
 * <p>
 * 语义注解    不是非常确定，建议不要使用，容易出错。
 * <p>
 * Forwarded Fields Annotation
 * 指定字段不经过任何处理，直接从input的某一个位置直接存储到output的某一个位置
 * > @ForwardedFields for single input functions such as Map and Reduce.
 * > @ForwardedFieldsFirst for the first input of a functions with two inputs such as Join and CoGroup.
 * > @ForwardedFieldsSecond for the second input of a functions with two inputs such as Join and CoGroup.
 * <p>
 * Non-Forwarded Fields Annotation
 * 与Forwarded Fields Annotation相反,指定字段需要经过处理，不能直接从input的某一个位置直接存储到output的某一个位置的所有字段
 * It is safe to declare a forwarded field as non-forwarded
 * Non-forwarded field information can only be specified for functions which have identical input and output types.(输入输出类型一致)
 * > @NonForwardedFields for single input functions such as Map and Reduce.
 * > @NonForwardedFieldsFirst for the first input of a function with two inputs such as Join and CoGroup.
 * > @NonForwardedFieldsSecond for the second input of a function with two inputs such as Join and CoGroup.
 * <p>
 * Read Fields Annotation
 * 指定函数需要访问及计算的所有字段
 * It is safe to declare a non-read field as read.
 * > @ReadFields for single input functions such as Map and Reduce.
 * > @ReadFieldsFirst for the first input of a function with two inputs such as Join and CoGroup.
 * > @ReadFieldsSecond for the second input of a function with two inputs such as Join and CoGroup.
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/23
 */
public class SemanticAnnotationsExample {
    public static void main(String[] args) throws Exception {
        // 创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //开启对象重用
        env.getConfig().enableObjectReuse();
        // 模拟数据源
        DataSource<Tuple3<String, Integer, Long>> input1 = env.fromElements(
                Tuple3.of("A", 300, 1557109591000L),
                Tuple3.of("A", 100, 1557109591000L),
                Tuple3.of("A", 200, 1557109593000L),
                Tuple3.of("B", 600, 1557109593000L),
                Tuple3.of("B", 700, 1557109594000L),
                Tuple3.of("B", 500, 1557109594000L),
                Tuple3.of("C", 400, 1557109599000L),
                Tuple3.of("C", 300, 1557109599000L)
        );
        DataSource<Tuple3<String, Integer, Long>> input2 = env.fromElements(
                Tuple3.of("A", 3, 1557109591000L),
                Tuple3.of("A", 1, 1557109591000L),
                Tuple3.of("A", 2, 1557109593000L),
                Tuple3.of("B", 6, 1557109593000L),
                Tuple3.of("B", 7, 1557109594000L),
                Tuple3.of("B", 5, 1557109594000L),
                Tuple3.of("C", 4, 1557109599000L),
                Tuple3.of("C", 3, 1557109599000L)
        );

        //Forwarded Fields Annotation
        /*input1.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Long, Integer>>() {
            private Tuple3<String, Long, Integer> tuple = new Tuple3<>();

            @Override
            public Tuple3<String, Long, Integer> map(Tuple3<String, Integer, Long> value) throws Exception {
                tuple.f0 = value.f0;
                tuple.f1 = value.f2;
                tuple.f2 = value.f1;
                return tuple;
            }
        }).withForwardedFields("f0;f1->f2;f2->f1")
                .print();*/

       /* input1.join(input2)
                .where(0, 2)
                .equalTo(0, 2)
                .with(new MyForwardedJoinFuncation())
                .print();*/

        //Non-Forwarded Fields Annotation   没有测试出异常情况的效果--->没有给出所有Non-Forwarded Fields
        /*input1.map(new MyMapFuncation()).print();*/

        // Read Fields Annotation    没有测试出异常情况的效果--->没有给出所有Read Fields
        input1.join(input2)
                .where(0, 2)
                .equalTo(0, 2)
                .with(new MyReadFieldsJoinFuncation())
                .print();
    }

    @FunctionAnnotation.ForwardedFieldsFirst("f0;f2->f3")
    @FunctionAnnotation.ForwardedFieldsSecond("f1")
    private static class MyForwardedJoinFuncation implements JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Long>> {
        private Tuple4<String, Integer, Integer, Long> tuple = new Tuple4<>();

        @Override
        public Tuple4<String, Integer, Integer, Long> join(Tuple3<String, Integer, Long> first, Tuple3<String, Integer, Long> second) throws Exception {
            tuple.f0 = first.f0;
            tuple.f1 = first.f1 / 2;
            tuple.f2 = second.f1;
            tuple.f3 = first.f2;
            return tuple;
        }
    }

    @FunctionAnnotation.NonForwardedFields("f0")
    private static class MyMapFuncation implements MapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> {
        private Tuple3<String, Integer, Long> tuple = new Tuple3<>();

        @Override
        public Tuple3<String, Integer, Long> map(Tuple3<String, Integer, Long> value) throws Exception {
            tuple.f0 = value.f0;
            tuple.f1 = value.f1 * value.f1;
            tuple.f2 = value.f2;
            return tuple;
        }
    }

    @FunctionAnnotation.ReadFieldsFirst("f1")
    @FunctionAnnotation.ReadFieldsSecond("f1")
    private static class MyReadFieldsJoinFuncation implements JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple4<String, Integer, Integer, Long>> {
        private Tuple4<String, Integer, Integer, Long> tuple = new Tuple4<>();

        @Override
        public Tuple4<String, Integer, Integer, Long> join(Tuple3<String, Integer, Long> first, Tuple3<String, Integer, Long> second) throws Exception {
            tuple.f0 = first.f0;
            tuple.f1 = first.f1 / 2;
            tuple.f2 = second.f1 * 2;
            tuple.f3 = first.f2;
            return tuple;
        }
    }
}
