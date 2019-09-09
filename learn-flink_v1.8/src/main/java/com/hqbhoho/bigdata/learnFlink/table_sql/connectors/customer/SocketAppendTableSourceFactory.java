package com.hqbhoho.bigdata.learnFlink.table_sql.connectors.customer;

import com.hqbhoho.bigdata.learnFlink.table_sql.concepts.stream.EventTimeTableExample;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * describe:
 *
 * 定义一个 基于Socket的TableSource
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/11
 */
public class SocketAppendTableSourceFactory implements StreamTableSourceFactory<Row> {

    /**
     *
     * @param properties : {@link UserDefinedTableFactoryExample.Socket#toConnectorProperties()}的返回值
     * @return
     */
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        String host = properties.get("host");
        int port = Integer.valueOf(properties.get("port"));
        // 之前定义的好的  基于socket的StreamTableSource
        return new EventTimeTableExample.UserActionSource(host, port);
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put("connector.property-version","1");
        context.put("update-mode", "append");
        context.put("connector.type", "socket");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> list = new ArrayList<>();
        list.add("host");
        list.add("port");
        return list;
    }

}
