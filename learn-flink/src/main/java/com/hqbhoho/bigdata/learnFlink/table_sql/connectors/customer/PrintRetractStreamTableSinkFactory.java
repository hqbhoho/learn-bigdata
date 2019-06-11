package com.hqbhoho.bigdata.learnFlink.table_sql.connectors.customer;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.*;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/11
 */
public class PrintRetractStreamTableSinkFactory implements StreamTableSinkFactory<Tuple2<Boolean,Row>> {

    @Override
    public StreamTableSink<Tuple2<Boolean,Row>> createStreamTableSink(Map<String, String> properties) {
        Optional.ofNullable("====="+properties).ifPresent(System.out::println);
        //  key 全部小写
        String[] fieldNames = properties.get("fieldnames").split(",");
        String[] fieldTypesName = properties.get("fieldtypes").split(",");
        TypeInformation<?>[] fieldTypes = new TypeInformation[fieldNames.length];
        for(int i = 0 ;i< fieldTypesName.length;i++){
            switch (fieldTypesName[i]){
                case "int" :
                    fieldTypes[i] = BasicTypeInfo.INT_TYPE_INFO;
                    break;
                case "string" :
                    fieldTypes[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
            }
        }
        return new UserDefinedSinksExample.RetractStreamPrintTableSink.Builder()
                .fieldNames(fieldNames)
                .fieldTypes(fieldTypes)
                .build();
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("connector.property-version","1");
        context.put("update-mode", "retract");
        context.put("connector.type", "print");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("fieldNames");
        list.add("fieldTypes");
        return list;
    }
}
