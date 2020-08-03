package com.sbcharr.database;

import java.sql.*;
import java.util.Properties;

public class DbOperations {
    private String jdbc_driver;
    private String db_url;
    private String db_user;
    private String db_pass;
    private String ssl;
    private Connection connection;
    private ResultSet resultSet;
    private PreparedStatement preparedStatement;


    public DbOperations(String jdbc_driver, String db_url, String db_user, String db_pass, String ssl) {
        this.jdbc_driver = jdbc_driver;
        this.db_url = db_url;
        this.db_user = db_user;
        this.db_pass = db_pass;
        this.ssl = ssl;
        this.connection = null;
    }

    public Connection getDBConnection() {
        Properties props = new Properties();
        props.setProperty("user",this.db_user);
        props.setProperty("password",this.db_pass);
        props.setProperty("ssl",this.ssl);

        try {
            // Register JDBC driver
            Class.forName(this.jdbc_driver);

            // Open a connection
            System.out.println("Connecting to database...");
            this.connection = DriverManager.getConnection(this.db_url, props);
            this.connection.setAutoCommit(false);
        } catch (Exception sqlException) {
            sqlException.printStackTrace();
        }
        return this.connection;
    }

    public void releaseDBResources() {
        try {
            if (this.connection != null) {
                System.out.println("closed db connection");
                this.connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void commitTransaction() {
        try {
            this.connection.commit();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }

    public void rollbackTransaction() {
        try {
            this.connection.rollback();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }

}
