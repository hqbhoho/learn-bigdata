package com.hqbhoho.bigdata.hive.parse1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.math.BigDecimal;
import java.security.PrivilegedAction;


/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/04/22
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String krb5_path = "E:\\javaProjects\\learn-bigdata\\learn-hive\\src\\main\\resources\\krb5.conf";
        String keytab_path = "E:\\javaProjects\\learn-bigdata\\learn-hive\\src\\main\\resources\\hive.keytab";
//        String krb5_path = "/etc/krb5.conf";
//        String keytab_path = "/etc/security/keytabs/hive-kylo.headless.keytab";
        System.setProperty("java.security.krb5.conf", krb5_path);

        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hive/ut01.hbzq.com@HBZQ.COM", keytab_path);

        } catch (Exception e) {
            new BigDecimal("");
            e.printStackTrace();
        }

        UserGroupInformation.getCurrentUser().doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                HiveCliDriver driver = new HiveCliDriver();
                try {
//                    driver.addSqls("INSERT OVERWRITE TABLE dim.dim_exchange_rate_d PARTITION (month_id ='${hiveconf:month_id}' , day_id='${hiveconf:day_id}') SELECT bz,hlbz,gsbl FROM (select bz,hlbz,gsbl, rq FROM (SELECT bz1 as bz,hlbz as hlbz,xhmcj as gsbl,CONCAT(month_id,day_id) AS rq FROM ods1.ods_aboss_datacenter_thlcs WHERE month_id = '${hiveconf:month_id}' AND day_id = '${hiveconf:day_id}' AND bz1=\"HKD\" AND bz2=\"RMB\" AND xhmcj > 0) t1) t2 WHERE t2.rq in (SELECT MAX(rq) FROM ( SELECT bz1 as bz,hlbz as hlbz,gsbl as gsbl,CONCAT(month_id,day_id) AS rq FROM ods1.ods_aboss_datacenter_thlcs WHERE month_id = '${hiveconf:month_id}' AND day_id = '${hiveconf:day_id}' AND bz1=\"USD\" AND bz2=\"RMB\" AND gsbl > 0 UNION ALL SELECT bz1 as bz,hlbz as hlbz,xhmcj as gsbl,CONCAT(month_id,day_id) AS rq FROM ods1.ods_aboss_datacenter_thlcs WHERE month_id = '${hiveconf:month_id}' AND day_id = '${hiveconf:day_id}' AND bz1=\"HKD\" AND bz2=\"RMB\" AND xhmcj > 0 UNION ALL SELECT  bz,hlbz,gsbl,CONCAT(month_id,day_id) AS rq FROM dim.dim_exchange_rate_d ) t3 ) ;");
//                    driver.addSqls("create table test.result1234 as select * from test.result");
                    driver.run(args);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });

    }
}
