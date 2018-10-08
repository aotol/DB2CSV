package com.zhanzhang;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Properties;

/**
 * This class connects to a database and dumps all the tables and contents out to CSV file
 */
public class DB2CSV {

    /** Dump the whole database to CSV files */
    public static String dumpDB(Properties props) {
        String driverClassName = props.getProperty("jdbc.driver.class");
        String driverURL = props.getProperty("jdbc.driver.url");
        String buffer = props.getProperty("buffer");
        int bufferSize = 1048576;// Default buffer size is 1024 * 1024 bytes (1MB)
        if (buffer!=null && buffer.trim().length()>0) {
        	try {
        		bufferSize = Integer.parseInt(buffer);
        	} catch (Exception e) {
        		
        	}
        }
        DatabaseMetaData dbMetaData = null;
        Connection dbConn = null;
        try {
            Class.forName(driverClassName);
            dbConn = DriverManager.getConnection(driverURL, props);
            dbMetaData = dbConn.getMetaData();
        }
        catch( Exception e ) {
            System.err.println("Unable to connect to database: " + e);
            return null;
        }

        try {
            StringBuffer result = new StringBuffer();
            String catalog = props.getProperty("catalog");
            String schema = props.getProperty("schemaPattern");
            String tables = props.getProperty("tableName");
            String exportLocation = props.getProperty("export.location");
            if (tables!=null && tables.trim().length()==0)
            	tables = null;
            if (exportLocation==null || exportLocation.trim().length()==0) {
            	System.err.println("Failure: export.location is not set");
                return null;
            }
            ResultSet rs = dbMetaData.getTables(catalog, schema, tables, null);
            if (! rs.next()) {
                System.err.println("Unable to find any tables matching: catalog=" + catalog + " schema=" + schema + " tables=" + tables);
                rs.close();
            } else {
                do {
                	//Get the table column titles
                    String tableName = rs.getString("TABLE_NAME");
                    System.out.println("Working on table: " + tableName);
                    String tableType = rs.getString("TABLE_TYPE");
                    if ("TABLE".equalsIgnoreCase(tableType)) {
                    	File file = new File(exportLocation + "/" + tableName + ".csv");
                    	if (file.exists()) {
                    		file.delete();
                    	}
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
                        dumpTable(dbConn, result, tableName, bufferSize, exportLocation);
                        writeToFile(tableName, result.toString(), exportLocation);
                        System.out.println("Table: " + tableName + " has been exported into: " + exportLocation);
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

    /** dump this particular table data 
     * @throws IOException */
    private static void dumpTable(Connection dbConn, StringBuffer result, String tableName, int bufferSize, String exportLocation) throws IOException {
        try {
            // First we output the create table stuff
            PreparedStatement stmt = dbConn.prepareStatement("SELECT * FROM " + tableName);
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
                    	int type = metaData.getColumnType(i+1);
                        if (type == Types.BLOB) {	//Process Blob text (Not LONGVARBINARY)
        					InputStream inputStream = ((Blob)value).getBinaryStream();
        					InputStreamReader isr=new InputStreamReader(inputStream);
        					BufferedReader br=new BufferedReader(isr);
        					String line;
        					while ((line=br.readLine())!=null) {
        						result.append("\"");	//Put double quote before and after string to escape comma
        						result.append(line);
        						result.append("\"");
        						result.append("\n");
        					}
        					inputStream.close();
                    	} else {
                    		result.append(value.toString());
                    	}
                    }
                }
                result.append("\n");
                if (result.length() >= bufferSize) {
                	writeToFile(tableName, result.toString(), exportLocation);
                	System.out.println(Calendar.getInstance().getTime() + " Exporting table: " + tableName + "'s " + result.length() / 1024 + "KB data into " + exportLocation + " to clean up memory buffer");
                	result.delete(0, result.length());
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println("Unable to dump table " + tableName + " because: " + e);
        }
    }
    
    private static void writeToFile(String tableName, String content, String exportLocation) throws IOException {
    	File directory = new File (exportLocation);
        if (!directory.exists()) {
        	directory.mkdirs();
        }
        File file = new File (directory.getAbsolutePath() + "/" +tableName + ".csv");
        if (!file.exists())
        	file.createNewFile();
        FileWriter fr = new FileWriter(file, true);	//Append the content to the end
        fr.write(content);
        fr.close();
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
            System.err.println("Unable to open property file: configuration.properties exception: " + e);
        }

    }
}