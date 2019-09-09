package com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka;

import com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/16
 */
public class KafkaAvroMessageDeserializationSchema implements DeserializationSchema<User> {

    @Override
    public User deserialize(byte[] message) throws IOException {
        User user = null;
        if (message == null)
            return user;
        // 使用Avro的java API 进行对象的反序列化
        SeekableByteArrayInput bis = new SeekableByteArrayInput(message);
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        try {
            DataFileReader<User> dataFileReader = new DataFileReader<User>(bis, userDatumReader);
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return user;
    }

    @Override
    public boolean isEndOfStream(User nextElement) {
        return false;
    }

    @Override
    public TypeInformation<User> getProducedType() {
        return TypeInformation.of(new TypeHint<User>() {
        });
    }
}
