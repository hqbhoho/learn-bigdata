package com.hqbhoho.bigdata.learnFlink.table_sql.concepts.common;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;

/**
 * describe:
 * <p>
 * A TableEnvironment maintains a catalog of tables which are registered by name.
 * but we can also use an External Catalog
 * An input table can be registered from various sources:
 * 1. an existing Table object, usually the result of a Table API or SQL query.
 * 2. a TableSource, which accesses external data, such as a file, database, or messaging system.
 * 3. a DataStream or DataSet from a DataStream or DataSet program. Registering a DataStream or DataSet is discussed in the Integration with DataStream and DataSet API section.
 * An output table can be registered using a TableSink.
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/27
 */
public class RegisterTableExample {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        //  Register a TableSource
        CsvTableSource csvTableSource = new CsvTableSource(
                "E:\\javaProjects\\learn-bigdata\\learn-flink\\src\\main\\resources\\info.csv",
                new String[]{"name", "account", "timestamp"},
                new TypeInformation[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO
                });
        tableEnv.registerTableSource("table_source_example", csvTableSource);
        //  Register a Table
        Table info = tableEnv.scan("table_source_example")
                .select("name,account");
        tableEnv.registerTable("info", info);
        // Data Process
        Table result = tableEnv.scan("info")
                .groupBy("name")
                .select("name,account.sum as sum");
        // Register a TableSink
        CsvTableSink csvTableSink = new CsvTableSink("E:\\javaProjects\\learn-bigdata\\learn-flink\\src\\main\\resources\\account.csv",
                ",",
                1,
                FileSystem.WriteMode.NO_OVERWRITE
        );
        tableEnv.registerTableSink(
                "table_sink_example",
                new String[]{"name", "account_sum"},
                new TypeInformation[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO
                },
                csvTableSink);
        // data output
        result.insertInto("table_sink_example");
        // run project
        env.execute("RegisterTableExample");
    }
}
