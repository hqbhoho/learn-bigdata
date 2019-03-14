package com.hqbhoho.bigdata.learnKafka.serialization;

import com.alibaba.fastjson.JSON;
import com.hqbhoho.bigdata.learnKafka.pojo.User;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * describe:
 * 通过Json的方式将byte数组反序列化成对象
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class UserJsonDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map map, boolean b) {
        // do nothing
    }

    @Override
    public User deserialize(String s, byte[] bytes) {
        if (bytes == null)
            return null;
        return JSON.parseObject(bytes, User.class);
    }

    @Override
    public void close() {
        // donothing
    }
}
