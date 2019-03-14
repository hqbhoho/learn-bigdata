package com.hqbhoho.bigdata.learnKafka.serialization;

import com.hqbhoho.bigdata.learnKafka.pojo.Item;
import com.hqbhoho.bigdata.learnKafka.pojo.User;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * describe:
 * 使用原生的方式反序列化对象
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class UserDeserializer implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //do nothing
    }

    @Override
    public User deserialize(String topic, byte[] data) {
        if (data == null)
            return null;

        if (data.length < 12)
            throw new SerializationException("The User data bytes length should not be less than 12.");

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int id = buffer.getInt();
        int nameLength = buffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        String name = new String(nameBytes);
        List<Item> items = new ArrayList<>();
        int itemsLength = buffer.getInt();
        while (itemsLength > 0) {
            int itemId = buffer.getInt();
            int itemNameLength = buffer.getInt();
            byte[] itemNameBytes = new byte[itemNameLength];
            buffer.get(itemNameBytes);
            String itemName = new String(itemNameBytes);
            Double itemPrice = buffer.getDouble();
            items.add(new Item(itemId, itemName, itemPrice));
            itemsLength -= (4 + 4 + itemNameLength + 8);
        }
        return new User(id, name, items);
    }

    @Override
    public void close() {
        //do nothing
    }
}
