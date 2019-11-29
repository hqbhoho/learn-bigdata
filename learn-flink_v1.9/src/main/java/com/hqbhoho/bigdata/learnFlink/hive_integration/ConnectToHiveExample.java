package com.hqbhoho.bigdata.learnFlink.hive_integration;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * describe:
 * <p>
 * Flink v1.9 begin to support Hive
 * Test Environment: Hive 1.1.0 with Kerberos
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/09
 */
public class ConnectToHiveExample {
    public static void main(String[] args) {
        // 构建Blink batch 环境
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

        // Connect to Hive Catalog

        String name            = "myhive";
        String defaultDatabase = "mydatabase";
        String hiveConfDir     = "E:\\javaProjects\\learn-bigdata\\learn-flink_v1.9\\src\\main\\resources\\hive-conf";
        String version         = "1.2.1"; // or 1.2.1

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        bbTableEnv.registerCatalog("myhive", hive);

        System.out.println(bbTableEnv.listTables());






    }
}
