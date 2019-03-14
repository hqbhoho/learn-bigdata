package com.hqbhoho.bigdata.learnKafka.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/13
 */
public class test {

    public static void main(String[] args) throws Exception {
        String schemaFilePath = test.class.getClassLoader().getResource("User.json").getPath();
        FileReader fr = new FileReader(new File(schemaFilePath));
        BufferedReader br = new BufferedReader(fr);
        StringBuilder sb = new StringBuilder();
        String line;
        while((line = br.readLine()) != null) {
            sb.append(line).append("\n");
        }
        String schemaStr = sb.toString();
        System.out.println(schemaStr);
        br.close();
        fr.close();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaStr);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("id", 1);
        avroRecord.put("name", "name" + 1);
        avroRecord.put("items", 22);
        byte[] avroRecordBytes = recordInjection.apply(avroRecord);



    }
}
