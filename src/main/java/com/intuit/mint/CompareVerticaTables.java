package com.sbcharr;

import com.sbcharr.database.DbOperations;
// import org.slf4j.LoggerFactory;
// import org.slf4j.Logger;

//import java.security.InvalidParameterException;
import java.sql.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.*;


/**
 * CompareVerticaTables
 *
 */
public class CompareVerticaTables {
    // private final Logger logger = LoggerFactory.getLogger(CompareVerticaTables.class.getName());

    static final String JDBC_DRIVER = "com.vertica.jdbc.Driver";
    private static String dbUrl;
    private static String dbUser;
    private static String dbPass;
    private static String dbCnnSSL;
    private ResultSet resultSet;
    private PreparedStatement preparedStatement;
    // private String sourceSchema;
    // private String targetSchema;
    private int daysToLookBack;
    static final String sourceSchema = "";
    static final String targetSchema = "";
    static String tablePropertiesFile = "tableconfig.properties";

    public CompareVerticaTables(int daysToLookBack) {
        this.daysToLookBack = daysToLookBack;
    }


    public HashMap<String, String> getTableConfig(String file) {
        HashMap<String, String> tableType = new HashMap<>();
        try {
            Properties tableConfig = new Properties();
            InputStream config = new FileInputStream(file);
            tableConfig.load(config);

            for (String key : tableConfig.stringPropertyNames()) {
                String value = tableConfig.getProperty(key);
                tableType.put(key, value);
            }
        } catch (IOException fe) {
            fe.printStackTrace();
        }
        return tableType;
    }

    public ArrayList<String> getTableColumns(Connection connection, String tableName, HashSet<String> ignoreColumns) throws SQLException {
        ArrayList<String> listOfColumns = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            String sqlGetTableInfo = "select concat(concat(column_name,':'),data_type) as column_name from v_catalog.columns where lower(table_schema) = '" + targetSchema + "' and lower(table_name) = ? order by ordinal_position;";
            preparedStatement = connection.prepareStatement(sqlGetTableInfo);
            preparedStatement.setString(1, tableName);
            resultSet = preparedStatement.executeQuery();

            while(resultSet.next()) {
                String tableColumn = resultSet.getString("column_name");
                if (!ignoreColumns.contains(tableColumn.split(":")[0])) {
                    // System.out.println(tableColumn);
                    listOfColumns.add(tableColumn);
                }
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                    preparedStatement.close();
                    // System.out.println("closed resultSet and statements");
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }
        }
        return listOfColumns;
    }

    public long getDiffRowCount(Connection connection, String query) throws SQLException {
        long diffRowCount = 0;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                diffRowCount = resultSet.getLong("diff_count");
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                    preparedStatement.close();
                    // System.out.println("closed resultSet and statements");
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }
        }
        return diffRowCount;
    }

    public long getNumRecords(Connection connection, String query) throws SQLException {
        long rowCount = 0;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(query);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                rowCount = resultSet.getLong("num_records");
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                    preparedStatement.close();
                    // System.out.println("closed resultSet and statements");
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }
        }
        return rowCount;
    }

    public void compareTables() throws SQLException {
        HashSet<String> ignoreColumns = new HashSet<>();
        Map<String, String> tableType = new HashMap<>();
        // add columns from table to ignore
        ignoreColumns.add("");


        // load properties file
        String userDirectory = System.getProperty("user.dir");
        String filePathSeparator = "/";
        String fileLocation = "src/main/resources/";
        String dbConfigFile = userDirectory + filePathSeparator + fileLocation + "dbconfig.properties";
        String tableConfigFile = userDirectory + filePathSeparator + fileLocation + tablePropertiesFile;

        try {

            InputStream dbCreds = new FileInputStream(dbConfigFile);
            Properties prop = new Properties();
            prop.load(dbCreds);
            // System.out.println(prop.getClass().getName());
            dbUrl = "jdbc:vertica://" + prop.getProperty("db.url") + ":" + prop.getProperty("db.port") + "/" + prop.getProperty("db.database");
            dbUser = prop.getProperty("db.user");
            dbPass = prop.getProperty("db.pass");
            dbCnnSSL = prop.getProperty("db.ssl");

            tableType = this.getTableConfig(tableConfigFile);
            // Print tables in scope from config
            // tableType.forEach((key, value) -> System.out.println(key + "=" + value));
        } catch (Exception fe) {
            fe.printStackTrace();
        }

        DbOperations dbOperations = new DbOperations(JDBC_DRIVER, dbUrl, dbUser, dbPass, dbCnnSSL);
        // connect to vertica db
        Connection verticaDbConnection = dbOperations.getDBConnection();
        List<List<Object>> records = new ArrayList<>();

        for (Map.Entry<String, String> entry : tableType.entrySet()) {
            // compare table count
            try {
                ArrayList<String> listOfColumns = this.getTableColumns(verticaDbConnection, entry.getKey(), ignoreColumns);
                ArrayList<String> validColumns = new ArrayList<>();

                for (String col : listOfColumns) {
                    String[] columnType = col.split(":");
                    if (columnType[1].toLowerCase().contains("varchar") || columnType[1].toLowerCase().contains("char") ) {
                        validColumns.add("case when lower(" + columnType[0] + ") in ('', 'null', 'nul') then null else " + columnType[0] + " end");
                    } else {
                        validColumns.add(columnType[0]);
                    }
                }
                String columnsInSelectClause = String.join(",", validColumns);
                //logger.info("Columns in select clause: " + columnsInSelectClause);
                long countLeftMinusRight = 0;
                long countRightMinusLeft = 0;
                long rowCountSource;
                long rowCountTarget;
                String sqlLeftMinusRight;
                String sqlRightMinusLeft;
                String sqlGetRowCountSource;
                String sqlGetRowCountTarget;
                String loadType;
                if (entry.getValue().equals("")) {
                    loadType = "full refresh";
                    System.out.println("executing " + loadType + " table: '" + entry.getKey() + "'");
                    sqlLeftMinusRight = "select count(*) as diff_count from (select " + columnsInSelectClause + " from " + sourceSchema + "." + entry.getKey() + " except select " + columnsInSelectClause + " from " + targetSchema + "." + entry.getKey() + ") A;";
                    System.out.println("sqlLeftMinusRight:" + sqlLeftMinusRight);

                    sqlRightMinusLeft = "select count(*) as diff_count from (select " + columnsInSelectClause + " from " + targetSchema + "." + entry.getKey() + " except select " + columnsInSelectClause + " from " + sourceSchema + "." + entry.getKey() + ") A;";
                    System.out.println("sqlRightMinusLeft:" + sqlRightMinusLeft);
                    sqlGetRowCountSource = "select count(*) as num_records from " + sourceSchema + "." + entry.getKey();
                    sqlGetRowCountTarget = "select count(*) as num_records from " + targetSchema + "." + entry.getKey();
                } else {
                    loadType = "incremental";
                    System.out.println("executing " + loadType + " table: '" + entry.getKey() + "'");

                    sqlLeftMinusRight = "select count(*) as diff_count from (select " + columnsInSelectClause + " from " + sourceSchema + "." + entry.getKey() + " where " + entry.getValue() + "::date = CURRENT_DATE - " + this.daysToLookBack + " except select " + columnsInSelectClause + " from " + targetSchema + "." + entry.getKey() + " where " + entry.getValue() + "::date = CURRENT_DATE - " + this.daysToLookBack + ") A;";
                    System.out.println("sqlLeftMinusRight:" + sqlLeftMinusRight);

                    sqlRightMinusLeft = "select count(*) as diff_count from (select " + columnsInSelectClause + " from " + targetSchema + "." + entry.getKey() + " where " + entry.getValue() + "::date = CURRENT_DATE - " + this.daysToLookBack + " except select " + columnsInSelectClause + " from " + sourceSchema + "." + entry.getKey() + " where " + entry.getValue() + "::date = CURRENT_DATE - " + this.daysToLookBack + ") A;";
                    System.out.println("sqlRightMinusLeft:" + sqlRightMinusLeft);

                    sqlGetRowCountSource = "select count(*) as num_records from " + sourceSchema + "." + entry.getKey() + " where " + entry.getValue() + "::date = CURRENT_DATE - " + this.daysToLookBack + ";";
                    sqlGetRowCountTarget = "select count(*) as num_records from " + targetSchema + "." + entry.getKey() + " where " + entry.getValue() + "::date = CURRENT_DATE - " + this.daysToLookBack + ";";
                }
                countLeftMinusRight = this.getDiffRowCount(verticaDbConnection, sqlLeftMinusRight);

                countRightMinusLeft = this.getDiffRowCount(verticaDbConnection, sqlRightMinusLeft);

                rowCountSource = this.getNumRecords(verticaDbConnection, sqlGetRowCountSource);
                rowCountTarget = this.getNumRecords(verticaDbConnection, sqlGetRowCountTarget);


                String sourceTableName = entry.getKey();

                String targetTableName = entry.getKey();
                LocalDate dataDate = LocalDate.now().minusDays(this.daysToLookBack);

                long diffRowCount = rowCountSource - rowCountTarget;
                // insert result into a vertica metadata table
                List<Object> record = new ArrayList<>();
                record.add(sourceSchema);
                record.add(sourceTableName);
                record.add(targetSchema);
                record.add(targetTableName);
                record.add(loadType);
                record.add(rowCountSource);
                record.add(rowCountTarget);
                record.add(rowCountSource - rowCountTarget);
                record.add(countLeftMinusRight);
                record.add(countRightMinusLeft);
                if (Math.abs(diffRowCount) + countLeftMinusRight + countRightMinusLeft == 0) {
                    record.add("match");
                } else {
                    record.add("mismatch");
                }
                record.add(dataDate);

                // add the record to the object[] array list
                records.add(record);

            } catch (SQLException e) {
                dbOperations.rollbackTransaction();
                e.printStackTrace();
                dbOperations.releaseDBResources();
                System.exit(1);
            }
        }
        // write results to a Vertica table
        try {
            writeToVertica(verticaDbConnection, records);
            dbOperations.commitTransaction();
        } catch (SQLException se) {
            dbOperations.rollbackTransaction();
            se.printStackTrace();
        }
        dbOperations.releaseDBResources();
    }

    public void writeToVertica(Connection verticaDbConnection, List<List<Object>> records) throws SQLException {
        for (List<Object> record : records) {
            // System.out.println(record.toString());
            String query = "insert into " + "schema.table(source_table_schema, source_table_name, target_table_schema, target_table_name, load_type, num_rows_source, num_rows_target, diff_record_count, num_records_mismatch_source, num_records_mismatch_target, parity_status, data_date) values("
                    + "'" + record.get(0) + "','" + record.get(1) + "','" + record.get(2) + "','" + record.get(3) + "','" + record.get(4) + "'," + record.get(5) + "," + record.get(6) + "," + record.get(7) + "," + record.get(8)
                    + "," + record.get(9) + ",'" + record.get(10) + "','" + record.get(11) + "');";
            // System.out.println(query);

            PreparedStatement preparedStatement = null;

            try {
                preparedStatement = verticaDbConnection.prepareStatement(query);
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new SQLException(e.getMessage());
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException se) {
                        se.printStackTrace();
                    }
                }
            }
        }
    }


    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        int daysToLookBack;
        if (args.length == 0) {
            daysToLookBack = 2;
        } else {
            daysToLookBack = Integer.parseInt(args[0]);
        }

        CompareVerticaTables compare = new CompareVerticaTables(daysToLookBack);
        try {
            compare.compareTables();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        long timeElasped = endTime - startTime;
        System.out.println("The program took " + (timeElasped/1000) + " seconds");
    }
}
