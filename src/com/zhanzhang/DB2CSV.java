package com.zhanzhang;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class connects to a database and dumps all the tables and contents out to CSV file
 */
public class DB2CSV {

    /** Dump the whole database to an SQL string */
    public static String dumpDB(Properties props) {
        String driverClassName = props.getProperty("jdbc.driver.class");
        String driverURL = props.getProperty("jdbc.driver.url");
        DatabaseMetaData dbMetaData = null;
        Connection dbConn = null;
        try {
            Class.forName(driverClassName);
            dbConn = DriverManager.getConnection(driverURL, props);
            dbMetaData = dbConn.getMetaData();
        }
        catch( Exception e ) {
            System.err.println("Unable to connect to database: "+e);
            return null;
        }

        try {
            StringBuffer result = new StringBuffer();
            String catalog = props.getProperty("catalog");
            String schema = props.getProperty("schemaPattern");
            String tables = props.getProperty("tableName");
            ResultSet rs = dbMetaData.getTables(catalog, schema, tables, null);
            if (! rs.next()) {
                System.err.println("Unable to find any tables matching: catalog="+catalog+" schema="+schema+" tables="+tables);
                rs.close();
            } else {
                do {
                	//Get the table column titles
                    String tableName = rs.getString("TABLE_NAME");
                    System.out.println("Working on table: " + tableName);
                    String tableType = rs.getString("TABLE_TYPE");
                    if ("TABLE".equalsIgnoreCase(tableType)) {
                        ResultSet tableMetaData = dbMetaData.getColumns(null, null, tableName, "%");
                        boolean firstLine = true;
                        while (tableMetaData.next()) {
                        	String columnName = tableMetaData.getString("COLUMN_NAME");
                        	if (firstLine) {
                        		firstLine = false;
                        		result.append(columnName);
                        	} else {
                        		result.append("," + columnName);
                        	}
                        }
                        if (result.length()>0) {
                        	result.append("\n");
                        }
                        tableMetaData.close();
                        // Populate the values
                        dumpTable(dbConn, result, tableName);
                        File directory = new File (props.getProperty("export.location"));
                        if (!directory.exists()) {
                        	directory.mkdirs();
                        }
                        File file = new File (directory.getAbsolutePath() + "/" +tableName+".csv");
                        if (!file.exists())
                        	file.createNewFile();
                        FileWriter fr = new FileWriter(file);
                        fr.write(result.toString());
                        fr.close();
                        System.out.println("Table: " + tableName + " has been exported into: " + file.getAbsolutePath());
                        result = new StringBuffer();
                    }
                } while (rs.next());
                rs.close();
            }
            dbConn.close();
            return result.toString();
        } catch (SQLException | IOException e) {
            e.printStackTrace();  //To change body of catch statement use Options | File Templates.
        }
        return null;
    }

    /** dump this particular table data */
    private static void dumpTable(Connection dbConn, StringBuffer result, String tableName) {
        try {
            // First we output the create table stuff
            PreparedStatement stmt = dbConn.prepareStatement("SELECT * FROM "+tableName);
            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            // Now we can output the actual data
            while (rs.next()) {
                for (int i=0; i<columnCount; i++) {
                    if (i > 0) {
                        result.append(",");
                    }
                    Object value = rs.getObject(i+1);
                    if (value == null) {
                        result.append("");
                    } else {
                        String outputValue = value.toString();
                        result.append(outputValue);
                    }
                }
                result.append("\n");
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println("Unable to dump table "+tableName+" because: "+e);
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        InputStream inputStream = DB2CSV.class.getResourceAsStream("configuration.properties");
        try {
            props.load(inputStream);
            System.out.println("Exporting database...");
            dumpDB(props);
            System.out.println("Done");
        } catch (IOException e) {
            System.err.println("Unable to open property file: configuration.properties exception: "+e);
        }

    }
}