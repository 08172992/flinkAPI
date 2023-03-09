package com.example.demo;


import java.sql.*;

public class postSQL {
    private String userName = "postgres";
    private String  passWord = "xyl123456";
    private String ipAddress = "81.68.219.199";
    private String port = "5432";
    private String databaseName = "bxdh";
    public static void main(String[] args) {
        String userName = "postgres";
        String  passWord = "xyl123456";
        String ipAddress = "81.68.219.199";
        String port = "5432";
        String databaseName = "bxdh";
        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://" + ipAddress + ":" + port + "/" + databaseName,
                            userName, passWord);
            System.out.println("连接成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }

    }

}
