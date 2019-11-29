package com.hqbhoho.bigdata.design.pattern.builder;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/11/29
 */
public class ConnectionDirector {
    private ConnectionBuilder connectionBuilder;

    public ConnectionDirector(ConnectionBuilder connectionBuilder) {
        this.connectionBuilder = connectionBuilder;
    }

    public Connection build(){
        connectionBuilder.buildDriver();
        connectionBuilder.buildUrl();
        connectionBuilder.buildUsername();
        connectionBuilder.buildPasswd();
        return connectionBuilder.getConnection();
    }
}
