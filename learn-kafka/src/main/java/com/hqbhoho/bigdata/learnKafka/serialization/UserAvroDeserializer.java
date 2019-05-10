package com.hqbhoho.bigdata.learnKafka.serialization;

import com.hqbhoho.bigdata.learnKafka.avro.User;
import com.hqbhoho.bigdata.learnKafka.producer.SerializerTestProducer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * describe:
 * 使用Avro框架进行对象反序列化
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class UserAvroDeserializer implements Deserializer<User> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SerializerTestProducer.class);

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // do nothing
    }

    @Override
    public User deserialize(String s, byte[] bytes) {
        User user = null;
        if (bytes == null)
            return user;
        // 使用Avro的java API 进行对象的反序列化
        SeekableByteArrayInput bis = new SeekableByteArrayInput(bytes);
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        try {
            DataFileReader<User> dataFileReader = new DataFileReader<User>(bis, userDatumReader);
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return user;
    }

    @Override
    public void close() {
        // do nothing
    }
}
