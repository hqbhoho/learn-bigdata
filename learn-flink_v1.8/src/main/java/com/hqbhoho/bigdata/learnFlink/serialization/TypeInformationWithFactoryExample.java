package com.hqbhoho.bigdata.learnFlink.serialization;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * describe:
 * <p>
 * //    @TypeInfo(MyTuple2InfoFactory.class)  注释这个注解的时候，会报错
 * Exception in thread "main" java.lang.UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig and type com.hqbhoho.bigdata.learnFlink.serialization.TypeInformationWithFactoryExample$MyTuple2 is treated as a generic type.
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/12
 */
public class TypeInformationWithFactoryExample {
    public static void main(String[] args) throws Exception {
        //构建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 禁用kyro serializer a built-in factory has highest precedence
        env.getConfig().disableGenericTypes();
        //模拟数据源
        DataSource<MyTuple2<String, Integer>> input = env.fromElements(
                new MyTuple2("A", 300),
                new MyTuple2("A", 100),
                new MyTuple2("A", 200),
                new MyTuple2("D", 300),
                new MyTuple2("F", 400),
                new MyTuple2("G", 400),
                new MyTuple2("H", 400),
                new MyTuple2("B", 600),
                new MyTuple2("B", 700),
                new MyTuple2("B", 500),
                new MyTuple2("C", 400),
                new MyTuple2("C", 300)
        );
        input.groupBy(i -> i.myField0).reduce(new ReduceFunction<MyTuple2<String, Integer>>() {
            @Override
            public MyTuple2<String, Integer> reduce(MyTuple2<String, Integer> value1, MyTuple2<String, Integer> value2) throws Exception {
                return new MyTuple2<>(value1.myField0, value1.myField1 + value2.myField1);
            }
        }).print();

    }

    /**
     * define type
     */
    @TypeInfo(MyTuple2InfoFactory.class)
    public static class MyTuple2<T0, T1> {
        public T0 myField0;
        public T1 myField1;

        public MyTuple2(T0 myField0, T1 myField1) {
            this.myField0 = myField0;
            this.myField1 = myField1;
        }

        @Override
        public String toString() {
            return "[ f1: " + myField0 + ",f2: " + myField1 + " ]";
        }
    }

    /**
     * define TypeInfo
     */
    private static class MyTuple2TypeInfo extends TypeInformation<MyTuple2> {
        private TypeInformation<?> t0;
        private TypeInformation<?> t1;

        public MyTuple2TypeInfo(TypeInformation<?> t0, TypeInformation<?> t1) {
            this.t0 = t0;
            this.t1 = t1;
        }

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public int getTotalFields() {
            return 2;
        }

        @Override
        public Class<MyTuple2> getTypeClass() {
            return MyTuple2.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<MyTuple2> createSerializer(ExecutionConfig config) {
            Optional.ofNullable("user define serializer be invoked ......")
                    .ifPresent(System.out::println);
            return new KryoSerializer<>(MyTuple2.class, config);
        }

        @Override
        public Map<String, TypeInformation<?>> getGenericParameters() {
            Map m = new HashMap();
            m.put("T0", this.t0);
            m.put("T1", this.t1);
            Optional.ofNullable(m).ifPresent(System.out::println);
            return m;
        }

        @Override
        public String toString() {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return t0.hashCode() + t1.hashCode();
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof MyTuple2;
        }
    }

    /**
     * define typeinfofactory
     */
    public static class MyTuple2InfoFactory extends TypeInfoFactory<MyTuple2> {

        @Override
        public TypeInformation<MyTuple2> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
            return new MyTuple2TypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
        }

    }


}
