package com.hqbhoho.bigdata.design.pattern.builder;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/29
 */
public interface ConnectionBuilder {
    void buildDriver();
    void buildUrl();
    void buildUsername();
    void buildPasswd();
    Connection getConnection();
}
