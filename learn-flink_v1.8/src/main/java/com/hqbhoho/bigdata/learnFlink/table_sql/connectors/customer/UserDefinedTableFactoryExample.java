package com.hqbhoho.bigdata.learnFlink.table_sql.connectors.customer;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * describe:
 * <p>
 * StreamTableSourceFactory  -->    基于socket的TableSource
 * StreamTableSinkFactoryink   -->  基于控制台print的TableSink
 * <p>
 * SPI规范，需要在classpath下目录 META-INF/services/下加入文件org.apache.flink.table.factories.TableFactory
 * 其内容为：TableFactory的具体实现全类名。
 * Test：
 * nc -l 19999
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * hqbhoho,200,1557109591000
 * hqbhoho,200,1557109592000
 * <p>
 * results:
 * (true,hqbhoho,200)
 * (false,hqbhoho,200)
 * (true,hqbhoho,400)
 * (false,hqbhoho,400)
 * (true,hqbhoho,600)
 * (false,hqbhoho,600)
 * (true,hqbhoho,800)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/06/11
 */
public class UserDefinedTableFactoryExample {
    public static void main(String[] args) throws Exception {
        // 获取配置参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host", "192.168.5.131");
        int port = tool.getInt("port1", 19999);
        // 创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 配置参数
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10000000);

        // 注册 table source
        tableEnv.connect(
                new Socket()
                        .host(host)
                        .port(port)
        )
                .inAppendMode()
                .registerTableSource("input");
        // 注册 table sink
        tableEnv.connect(
                new Print()
                        .fieldNames("name,account")
                        .fieldTypes("string,int")
        )
                .inRetractMode()
                .registerTableSink("output");

        tableEnv.sqlUpdate("insert into output select name,sum(account) from input group by name");
        env.execute("UserDefinedTableFactoryExample");


    }

    /**
     * User Defined Source Connector
     */
    public static class Socket extends ConnectorDescriptor {
        private String host;
        private int port;

        public Socket() {
            super("socket", 1, false);
        }

        public Socket host(String host) {
            this.host = host;
            return this;
        }

        public Socket port(int port) {
            this.port = port;
            return this;
        }


        @Override
        protected Map<String, String> toConnectorProperties() {
            Map<String, String> properties = new HashMap<>();
            properties.put("host", this.host);
            properties.put("port", this.port + "");
            return properties;
        }
    }

    /**
     * User Defined Sink Connector
     */
    public static class Print extends ConnectorDescriptor {
        private String fieldNames;
        private String fieldTypes;

        public Print() {
            super("print", 1, false);
        }

        public Print fieldNames(String fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Print fieldTypes(String fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }


        @Override
        protected Map<String, String> toConnectorProperties() {
            Map<String, String> properties = new HashMap<>();
            properties.put("fieldNames", this.fieldNames);
            properties.put("fieldTypes", this.fieldTypes);
            Optional.ofNullable(">>>>>" + properties).ifPresent(System.out::println);
            return properties;
        }
    }
}
