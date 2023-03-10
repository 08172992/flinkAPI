package com.example.demo;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
/**封装数据库连接和关闭*/
public class JDBCUtils {
    public static Connection getConnection() throws Exception{

        final String url = "jdbc:mysql://81.68.219.199:3306/flink_webSocket?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=GMT%2B8";
        final String name = "com.mysql.cj.jdbc.Driver";
        final String user = "root";
        final String password = "Xyl@123456";
        Connection conn = null;
        Class.forName(name);//指定连接类型
        conn = DriverManager.getConnection(url, user, password);//获取连接
        if (conn!=null) {
            System.out.println("获取连接成功");
        }else {
            System.out.println("获取连接失败");
        }
        return conn;
    }
    public static void closeResource(Connection conn, Statement ps){
        try {
            if(ps != null)
                ps.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
        try {
            if(conn != null)
                conn.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
    public static void closeResource(Connection conn, Statement ps, ResultSet rs){
        try {
            if(ps != null)
                ps.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
        try {
            if(conn != null)
                conn.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
        try {
            if(rs!=null)
                rs.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}
