package com.hqbhoho.bigdata.hive.parse1.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCClient {
    private  String jdbcDriver;
    private  String connectionUrl;
    private  String username;
    private  String password;

    public static JDBCClient getMysqlClient(){
        return new JDBCBuilder().jdbcDriver("com.mysql.jdbc.Driver")
                .connectionUrl("jdbc:mysql://10.105.1.182:3306/")
                .username("root")
                .password("123456")
                .build();
    }

    private JDBCClient(JDBCBuilder builder){
        this.jdbcDriver=builder.jdbcDriver;
        this.connectionUrl=builder.connectionUrl;
        this.username=builder.username;
        this.password=builder.password;
    }

    public static class JDBCBuilder{

        private  String jdbcDriver;
        private  String connectionUrl;
        private  String username;
        private  String password;

        public JDBCBuilder jdbcDriver(String jdbcDriver){
            this.jdbcDriver=jdbcDriver;
            return this;
        }

        public JDBCBuilder connectionUrl(String connectionUrl){
            this.connectionUrl=connectionUrl;
            return this;
        }

        public JDBCBuilder username(String username){
            this.username=username;
            return this;
        }

        public JDBCBuilder password(String password){
            this.password=password;
            return this;
        }

        public JDBCClient build(){
            return new JDBCClient(this);
        }

    }

    /**
     * 获取数据库连接
     * @return Impala连接对象
     */
    public Connection getConnection() throws Exception{
        Class.forName(jdbcDriver);
//        System.out.println(this.username+this.password);
        return  DriverManager.getConnection(this.connectionUrl,this.username,this.password);
    }

    /**
     * 根据查询sql,返回查询的数据记录
     * @param sql
     * @param params  sql中的参数
     * @return
     * @throws IOException
     */
    public List<String> findRecordsByCondition(final String sql, List<String> params)  {
        ArrayList<String> list = new ArrayList<>();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        FileWriter fw = null;
        Connection conn = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            //填充查询的sql参数
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setString(i + 1, params.get(i));
                }
            }
            rs = pstmt.executeQuery();
            //获取查询出来的元数据
              ResultSetMetaData metaData = rs.getMetaData();
              int columnCount = metaData.getColumnCount();
//            StringBuilder columnList = new StringBuilder();
//            for (int i = 1; i <= columnCount; i++) {
//                if (i == columnCount) {
//                    columnList.append(metaData.getColumnName(i));
//                } else {
//                    columnList.append(metaData.getColumnName(i) + ",");
//                }
//            }
//            list.add(columnList.toString());
            //获取查询记录
            while (rs.next()) {
                StringBuilder columnValue = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    if (i == columnCount) {
                        columnValue.append(rs.getString(i));
                    } else {
                        columnValue.append(rs.getString(i) + ",");
                    }
                }
                list.add(columnValue.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            release(conn, pstmt, rs);
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }

    /**
     * 根据查询sql,返回查询的数据记录
     * @param sql
     * @param params  sql中的参数
     * @return
     * @throws IOException
     */
    public void insertRecords(final String sql, List<Object> params)  {
        ArrayList<String> list = new ArrayList<>();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        FileWriter fw = null;
        Connection conn = null;
        try {

            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
//            pstmt.setBinaryStream(1,new ByteArrayInputStream(params.get(0).getBytes()));
//            pstmt.setAsciiStream(1,new ByteArrayInputStream(params.get(0).getBytes()));
            StringReader reader = new StringReader(params.get(2).toString());
            pstmt.setCharacterStream(3, reader, params.get(2).toString().length());
            pstmt.setString(2,params.get(1).toString());
            pstmt.setInt(1, (Integer) params.get(0));
            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            release(conn, pstmt, rs);
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }




    /**
     * 释放资源
     * @param conn
     * @param pstmt
     * @param rs
     */
    public void release(Connection conn, PreparedStatement pstmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }



}
