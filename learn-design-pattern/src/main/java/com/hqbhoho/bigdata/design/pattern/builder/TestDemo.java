package com.hqbhoho.bigdata.design.pattern.builder;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/29
 */
public class TestDemo {
    public static void main(String[] args) {


        MysqlConnectionBuilder mysqlConnectionBuilder =
                new MysqlConnectionBuilder();
        Connection connection1 = new ConnectionDirector(mysqlConnectionBuilder).build();
        System.out.println(connection1);

        System.out.println("====================================================");

        OracleConnectionBuilder oracleConnectionBuilder =
                new OracleConnectionBuilder();
        Connection connection2 = new ConnectionDirector(oracleConnectionBuilder).build();
        System.out.println(connection2);


    }
}
