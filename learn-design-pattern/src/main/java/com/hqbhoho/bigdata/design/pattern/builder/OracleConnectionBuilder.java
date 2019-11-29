package com.hqbhoho.bigdata.design.pattern.builder;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/29
 */
public class OracleConnectionBuilder implements ConnectionBuilder {

    private Connection conn;

    public OracleConnectionBuilder() {
        this.conn = new Connection();
    }

    @Override
    public void buildDriver() {
        System.out.println("正在设置Oracle数据库driver信息...");
        this.conn.setDriver("oracle.jdbc.driver.OracleDriver");
    }

    @Override
    public void buildUrl() {
        System.out.println("正在读取Oracle数据库Url配置...");
        this.conn.setUrl("jdbc:oracle:thin:@localhost:1521:orcl");
    }

    @Override
    public void buildUsername() {
        System.out.println("正在读取Oracle数据库user配置...");
        this.conn.setUrl("oracle");
    }

    @Override
    public void buildPasswd() {
        System.out.println("正在读取Oracle数据库passwd配置...");
        this.conn.setPasswd("123456");
    }

    @Override
    public Connection getConnection() {
        return this.conn;
    }
}
