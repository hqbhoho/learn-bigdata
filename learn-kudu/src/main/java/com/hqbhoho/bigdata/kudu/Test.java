package com.hqbhoho.bigdata.kudu;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.*;

import org.apache.hadoop.conf.Configuration;
import java.math.BigDecimal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/06/29
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String krb5_path = "E:\\javaProjects\\learn-bigdata\\learn-hive\\src\\main\\resources\\krb5.conf";
        String keytab_path = "E:\\javaProjects\\learn-bigdata\\learn-kudu\\src\\main\\resources\\octopus.keytab";
//        String krb5_path = "/etc/krb5.conf";
//        String keytab_path = "/etc/security/keytabs/hive-kylo.headless.keytab";
        System.setProperty("java.security.krb5.conf", krb5_path);

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("octopus@HBZQ.COM", keytab_path);

        } catch (Exception e) {
            new BigDecimal("");
            e.printStackTrace();
        }

        UserGroupInformation.getCurrentUser().doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    String masters = "ut01.hbzq.com";
                    KuduClient client = new KuduClient.KuduClientBuilder(masters).build();
                    KuduTable table = client.openTable("impala::default.my_first_table");
                    // 获取需要查询数据的列
                    List<String> projectColumns = new ArrayList<String>();
                    projectColumns.add("id");
                    projectColumns.add("name");

                    KuduScanner scanner = client.newScannerBuilder(table)
                            .setProjectedColumnNames(projectColumns)
                            .build();
                    while (scanner.hasMoreRows()) {
                        RowResultIterator results = scanner.nextRows();

                        // 18个tablet，每次从tablet中获取的数据的行数
                        int numRows = results.getNumRows();
                        System.out.println("numRows is : " + numRows);
                        while (results.hasNext()) {
                            RowResult result = results.next();
                            long id = result.getLong(0);
                            String name = result.getString(1);
                            System.out.println("id is : " + id + "===name is : " + name);
                        }
                    }
                } catch (Exception e){

                }
                return null;
            }
        });



    }
}
