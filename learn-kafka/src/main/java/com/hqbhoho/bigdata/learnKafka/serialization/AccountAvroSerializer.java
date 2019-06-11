package com.hqbhoho.bigdata.learnKafka.serialization;

import com.hqbhoho.bigdata.learnKafka.avro.Account;
import com.hqbhoho.bigdata.learnKafka.producer.SerializerTestProducer;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * describe:
 * 使用Avro框架进行对象序列化
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class AccountAvroSerializer implements Serializer<Account> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SerializerTestProducer.class);

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // do nothing
    }

    @Override
    public byte[] serialize(String s, Account account) {
        if (account == null) {
            return null;
        }
        // 使用Avro的java API 进行对象的序列化
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<Account> writer = new SpecificDatumWriter<>(Account.class);
        DataFileWriter<Account> dataFileWriter = new DataFileWriter<Account>(writer);
        try {
            dataFileWriter.create(account.getSchema(), out);
            dataFileWriter.append(account);
            dataFileWriter.close();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }

    @Override
    public void close() {
        // do nothing
    }
}
