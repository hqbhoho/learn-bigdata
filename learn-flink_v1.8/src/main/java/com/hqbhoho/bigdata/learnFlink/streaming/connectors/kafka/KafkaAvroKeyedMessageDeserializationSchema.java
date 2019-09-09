package com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka;

import com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * describe:
 * <p>
 * Kafka message Deserialization avro
 * org.apache.avro.AvroRuntimeException: Malformed data. Length is negative: -40  应该是消息中带了schema信息
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/17
 */
public class KafkaAvroKeyedMessageDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, User>> {
    @Override
    public boolean isEndOfStream(ConsumerRecord nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<String, User> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

        String key = new String(record.key());
        byte[] message = record.value();
        User user = null;
        if (message != null) {
            try {
                // 使用Avro的java API 进行对象的反序列化
                SeekableByteArrayInput bis = new SeekableByteArrayInput(message);
                DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
                DataFileReader<User> dataFileReader = new DataFileReader<User>(bis, userDatumReader);
                while (dataFileReader.hasNext()) {
                    user = dataFileReader.next(user);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new ConsumerRecord<String, User>(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.timestampType(),
                record.checksum(),
                record.serializedKeySize(),
                record.serializedValueSize(),
                key,
                user,
                record.headers());
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, User>>() {
        });
    }
}
