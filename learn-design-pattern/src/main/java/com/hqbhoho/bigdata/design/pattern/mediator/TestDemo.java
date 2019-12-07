package com.hqbhoho.bigdata.design.pattern.mediator;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/12/07
 */
public class TestDemo {
    public static void main(String[] args) {
        DataX dataX = new DataX();

        Mysql mysql = new Mysql();
        Redis redis = new Redis();
        ES es = new ES();

        dataX.register("mysql",mysql);
        dataX.register("es",es);
        dataX.register("redis",redis);


        System.out.println("================add data to mysql===============");
        mysql.addData("1");
        System.out.println("mysql: "+mysql.selectAll());
        System.out.println("es: "+es.selectAll());
        System.out.println("redis: "+redis.selectAll());
        System.out.println("================add data redis===============");
        redis.addData("2");
        System.out.println("mysql: "+mysql.selectAll());
        System.out.println("es: "+es.selectAll());
        System.out.println("redis: "+redis.selectAll());
        System.out.println("================add data to es===============");
        es.addData("3");
        System.out.println("mysql: "+mysql.selectAll());
        System.out.println("es: "+es.selectAll());
        System.out.println("redis: "+redis.selectAll());

    }
}
