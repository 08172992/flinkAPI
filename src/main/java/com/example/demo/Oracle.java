package com.example.demo;


import java.sql.*;

public class Oracle {
    public static void main(String[]args) throws Exception {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String url="jdbc:oracle:thin:@localhost:1521:orcl";
        String uname ="scott";
        String pwd = "scott";
        Connection con = DriverManager.getConnection(url,uname,pwd);
        System.out.println("连接成功");
        System.out.println(con.getClass().getName());

    }
}
