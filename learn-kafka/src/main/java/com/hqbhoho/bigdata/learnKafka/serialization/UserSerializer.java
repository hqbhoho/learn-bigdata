package com.hqbhoho.bigdata.learnKafka.serialization;

import com.hqbhoho.bigdata.learnKafka.pojo.Item;
import com.hqbhoho.bigdata.learnKafka.pojo.User;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * describe:
 * 使用原生的方式序列化对象
 * 详解  除了int(4),double(8)等具有固定的长度     其他可变的类型，需要加入一个长度标识
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class UserSerializer implements Serializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //do nothing
    }

    @Override
    public byte[] serialize(String topic, User data) {
        if (data == null)
            return null;

        int id = data.getId();
        String name = data.getName();
        List<Item> items = data.getItems();

        byte[] nameBytes;
        int total = 0;
        if (name != null)
            nameBytes = name.getBytes();
        else
            nameBytes = new byte[0];
        if (items != null)
            for (Item item : items) {
                int itemId = item.getId();
                byte[] itemNameBytes = item.getName().getBytes();
                Double price = item.getPrice();
                total += (4 + 4 + itemNameBytes.length + 8);
            }
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + nameBytes.length + 4 + total);
        buffer.putInt(id);
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);
        if (total != 0)
            buffer.putInt(total);
        for (Item item : items) {
            int itemId = item.getId();
            byte[] itemNameBytes = item.getName().getBytes();
            Double price = item.getPrice();
            buffer.putInt(itemId);
            buffer.putInt(itemNameBytes.length);
            buffer.put(itemNameBytes);
            buffer.putDouble(price);
        }
        return buffer.array();
    }

    @Override
    public void close() {
        //do nothing
    }
}
