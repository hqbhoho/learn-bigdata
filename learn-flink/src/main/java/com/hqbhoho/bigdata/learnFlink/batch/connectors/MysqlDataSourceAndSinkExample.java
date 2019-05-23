package com.hqbhoho.bigdata.learnFlink.batch.connectors;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Types;

/**
 * describe:
 * <p>
 * DataSet operator Mysql
 * <p>
 * 统计商品卖出数量,总销售额
 * items
 * +---------+------------+----------+
 * | item_id | item_price | item_num |
 * +---------+------------+----------+
 * |    1001 |      25.93 |       10 |
 * |    1002 |      45.40 |       40 |
 * |    1003 |      10.42 |       50 |
 * +---------+------------+----------+
 * account
 * +------+---------+
 * | num  | sum     |
 * +------+---------+
 * |  100 | 2596.30 |
 * +------+---------+
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/21
 */
public class MysqlDataSourceAndSinkExample {
    public static void main(String[] args) throws Exception {
        // 创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Row> input = env.createInput(
                JDBCInputFormat.buildJDBCInputFormat()
                        .setDBUrl("jdbc:mysql://192.168.5.131:3306/test")
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setUsername("root")
                        .setPassword("123456")
                        .setQuery("select * from test.items")
                        .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.BIG_DEC_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                        .finish()

        );
        DataSet<Row> result = input.reduceGroup(new GroupReduceFunction<Row, Row>() {
            @Override
            public void reduce(Iterable<Row> values, Collector<Row> out) throws Exception {
                Integer num = 0;
                BigDecimal sum = new BigDecimal(0.00);
                for (Row value : values) {
                    int num_tmp = (Integer) value.getField(2);
                    BigDecimal price = (BigDecimal) value.getField(1);
                    num += num_tmp;
                    sum = sum.add(price.multiply(new BigDecimal(num_tmp)));
                }
                out.collect(Row.of(num, sum));

            }
        }).returns(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.BIG_DEC_TYPE_INFO));
//        result.print();
        result.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDBUrl("jdbc:mysql://192.168.5.131:3306/test")
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setUsername("root")
                        .setPassword("123456")
                        .setQuery("insert into test.account(num,sum) values (?,?)")
                        .setSqlTypes(new int[]{Types.INTEGER, Types.DECIMAL})
                        .finish()
        );
        env.execute("MysqlDataSourceAndSinkExample");
    }
}
