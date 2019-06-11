package com.hqbhoho.bigdata.learnKafka.serialization;

import com.alibaba.fastjson.JSON;
import com.hqbhoho.bigdata.learnKafka.pojo.Account;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * describe:
 * 通过Json的方式将对象序列化成byte数组
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class AccountJsonSerializer implements Serializer<Account> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //do nothing
    }

    @Override
    public byte[] serialize(String topic, Account account) {
        if (account == null)
            return null;
        return JSON.toJSONBytes(account);
    }

    @Override
    public void close() {
        //do nothing
    }
}
