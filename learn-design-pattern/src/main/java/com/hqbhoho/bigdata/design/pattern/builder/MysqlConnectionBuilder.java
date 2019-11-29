package com.hqbhoho.bigdata.design.pattern.builder;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/29
 */
public class MysqlConnectionBuilder implements ConnectionBuilder {

    private Connection conn;

    public MysqlConnectionBuilder(){
        this.conn = new Connection();
    }

    @Override
    public void buildDriver() {
        System.out.println("正在设置MySQL数据库driver信息...");
        this.conn.setDriver("com.mysql.jdbc.Driver");
    }

    @Override
    public void buildUrl() {
        System.out.println("正在读取MySQL数据库Url配置...");
        this.conn.setUrl("jdbc:mysql://10.105.1.182:3306/test");
    }

    @Override
    public void buildUsername() {
        System.out.println("正在读取MySQL数据库user配置...");
        this.conn.setUrl("mysql");
    }

    @Override
    public void buildPasswd() {
        System.out.println("正在读取MySQL数据库passwd配置...");
       this.conn.setPasswd("123456");
    }

    @Override
    public Connection getConnection() {
        return this.conn;
    }
}
