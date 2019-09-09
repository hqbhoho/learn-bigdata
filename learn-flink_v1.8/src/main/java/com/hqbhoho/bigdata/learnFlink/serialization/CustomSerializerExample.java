package com.hqbhoho.bigdata.learnFlink.serialization;

import com.hqbhoho.bigdata.learnFlink.serialization.protobuf.ProtobufModel;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Optional;

/**
 * describe:
 * <p>
 * protobuf class serializer
 * 沒有测试出效果？？？？？？？？？？？？？？？？？？？？？？？？？  待后续解决   这里是应为使用的是java 客户端的对象？？？
 *
 * env.getConfig().registerTypeWithKryoSerializer(ProtobufModel.Account.class, ProtobufSerializer.class);不加也能运行代码
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/13
 */
public class CustomSerializerExample {
    public static void main(String[] args) throws Exception {
        //构建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.disableOperatorChaining();
//        env.setParallelism(1);
//        env.getConfig().enableForceKryo();
//        env.getConfig().disableGenericTypes();
//        env.getConfig().disableAutoTypeRegistration();
//        env.getConfig().registerTypeWithKryoSerializer(ProtobufModel.Account.class, ProtobufSerializer.class);
        env.addSource(new MyDataSource(),"source").keyBy(i->i.getName()).reduce(new ReduceFunction<ProtobufModel.Account>() {
            @Override
            public ProtobufModel.Account reduce(ProtobufModel.Account value1, ProtobufModel.Account value2) throws Exception {
                Optional.ofNullable("Thread ID: "+Thread.currentThread().getId()+",process "+value2)
                        .ifPresent(System.out::println);
                return ProtobufModel.Account.newBuilder()
                        .setName(value1.getName())
                        .setAccount(value1.getAccount()+value2.getAccount())
                        .setTimestamp(System.currentTimeMillis())
                        .build();
            }
        }).print();
        env.execute("CustomSerializerExample");
    }
}
