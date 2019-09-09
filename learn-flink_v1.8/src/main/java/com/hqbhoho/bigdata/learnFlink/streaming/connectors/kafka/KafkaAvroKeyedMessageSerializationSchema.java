package com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka;

import com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka.avro.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * describe:
 * <p>
 * Kafka message Serialization avro
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/17
 */
public class KafkaAvroKeyedMessageSerializationSchema implements KeyedSerializationSchema<User> {
    @Override
    public byte[] serializeKey(User element) {
        return element.getId().toString().getBytes();
    }

    @Override
    public byte[] serializeValue(User element) {
        if (element == null) {
            return null;
        }
        // 使用Avro的java API 进行对象的序列化
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<User> writer = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(writer);
        try {
            dataFileWriter.create(element.getSchema(), out);
            dataFileWriter.append(element);
            dataFileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    @Override
    public String getTargetTopic(User element) {
        return null;
    }
}
